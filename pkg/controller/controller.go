package controller

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/binbinpeng123/blitz/pkg"
	"github.com/binbinpeng123/blitz/pkg/dao"
	"github.com/petermattis/goid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/client-go/informers"
	informerv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	InfraComputingLabelKey = "infra-computing"
	InfraComputingLabelVal = "v2"
)

const (
	podResyncPeriod      = 30 * time.Minute
	labelJobID           = "job_id"
	PodStatusIndex       = "podStatus"
	PodEventIndex        = "podEvent"
	annotationRetryRound = "retry_round"
)

type Controller struct {
	db                     *dao.Dao
	submitingNSsWithCancel map[string]context.CancelFunc
	k8s                    string

	// informerSynced 标记 informer initial list 是否完成。用于区分 AddFunc 触发来源：
	// false → 初次 list（reconcile metric trigger=startup）；true → 后续 watch 事件。
	informerSynced atomic.Bool

	// k8s
	kubeClient  kubernetes.Interface
	podQueue    workqueue.RateLimitingInterface
	podInformer informerv1.PodInformer
	evInformer  informerv1.EventInformer
	evQueue     workqueue.RateLimitingInterface
	nsListener  listersv1.NamespaceLister
}

func NewController(config *rest.Config, kubeClient kubernetes.Interface, k8s string) (*Controller, error) {
	// 获取数据库实例
	db := dao.Get(k8s)
	if db == nil {
		return nil, fmt.Errorf("database not initialized for cluster %s", k8s)
	}

	ctrl := &Controller{
		db:         db,
		kubeClient: kubeClient,
		k8s:        k8s,
		podQueue:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "pods"),
		evQueue:    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "ev"),
	}

	return ctrl, nil
}

// Run 启动控制器主循环
// 该方法初始化所有Informer和Worker，并开始处理事件
// 参数:
//   - ctx: 上下文，用于控制控制器生命周期
//   - workers: Worker协程数量
func (ctrl *Controller) Run(ctx context.Context, workers int) {
	// 1.处理panic
	defer runtime.HandleCrash()

	log.Info("Starting workload-controller Controller")
	log.WithField("worker", workers).Info("Current Worker Numbers")

	// 2.Pod Informer使用单独的Factory，以便设置LabelSelector过滤非任务的Pod，避免太多系统Pod变更影响性能
	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(ctrl.kubeClient, podResyncPeriod, informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
		lo.LabelSelector = fmt.Sprintf("%s=%s", InfraComputingLabelKey, InfraComputingLabelVal)
	}))
	ctrl.podInformer = podInformerFactory.Core().V1().Pods()
	ctrl.initPodInformer()

	// 3.创建K8S Informer
	k8sFactory := informers.NewSharedInformerFactory(ctrl.kubeClient, podResyncPeriod)
	ctrl.evInformer = k8sFactory.Core().V1().Events()
	ctrl.initEventInformer()

	ctrl.nsListener = k8sFactory.Core().V1().Namespaces().Lister()

	// 4.启动informer
	k8sFactory.Start(ctx.Done())
	podInformerFactory.Start(ctx.Done())
	log.Info("Kubernetes informers started")

	// 5.等待List-Watch机制同步数据完成
	c, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	log.Info("Waiting for informer caches to sync...")
	if !cache.WaitForCacheSync(
		c.Done(),
		ctrl.podInformer.Informer().HasSynced,
		ctrl.evInformer.Informer().HasSynced) {
		log.Fatal("Timed out waiting for caches to sync")
	}
	log.Info("All informer caches synced successfully")

	// 启动 Worker：startWorker 内部用 wait.UntilWithContext 同步运行 + WaitGroup join，
	// 让 Run() 在 ctx cancel 后能等所有 worker 退完再继续 drain barrier。
	var workWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		ctrl.startWorker(ctx, &workWG, ctrl.runPodWorker)
	}
	log.WithField("workers", workers).Info("Worker goroutines started")

	// cron
	log.Info("Cron jobs started")

	// SIGTERM → signal.NotifyContext 让 ctx 取消，解除以下 <-ctx.Done() 阻塞。
	<-ctx.Done()
	log.Info("SIGTERM received, draining workers...")

	// drain barrier 严格三步序：
	//   1. ShutDown 全部 queue，worker Get() 立即返回 quit=true；
	//   2. 等 worker 退出（drainTO 兜底）；
	//   3. worker 退完后再关 billingWriter，避免与 in-flight Write 竞争。
	ctrl.podQueue.ShutDown()
	ctrl.evQueue.ShutDown()

	ctrl.drainWorkers(&workWG, 25*time.Second)

	log.Info("Controller shutdown complete")
}

// drainWorkers 等所有 worker WaitGroup Done，超时递增 ShutdownForceTotal。
// 生产 timeout 25s（默认 30s grace 减去 5s 给 billingWriter.Close），测试可缩短。
func (ctrl *Controller) drainWorkers(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Info("all workers drained")
	case <-time.After(timeout):
		log.Warn("worker drain timeout, force closing")
	}
}

func (ctrl *Controller) runPodWorker(ctx context.Context) {
	defer runtime.HandleCrash()
	gid := goid.Get()
	for ctrl.processNextItem(ctx, gid, ctrl.podQueue, ctrl.podInformer.Informer().GetIndexer(), ctrl.operatePod) {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (ctrl *Controller) processNextItem(ctx context.Context, _ int64, queue workqueue.RateLimitingInterface, indexer cache.Indexer, operate func(ctx context.Context, obj interface{}, key string, exists bool) (bool, error)) bool {
	queueGetStart := time.Now()
	item, quit := queue.Get()
	queueGetMs := time.Since(queueGetStart).Milliseconds()
	if quit {
		log.Infof("workqueue done, last key:%s", item)
		return false
	}
	defer queue.Done(item)

	key := item.(string)
	logger := log.WithField("workload", key)

	indexerGetStart := time.Now()
	obj, exists, err := indexer.GetByKey(key)
	indexerGetMs := time.Since(indexerGetStart).Milliseconds()
	if err != nil {
		logger.WithField("error", err).Error("Failed to get workload from informer")
		return true
	}
	// 通过 ctx 把 queue.Get / indexer.GetByKey 的耗时透传给 operate。
	// 只 operateVJ 会消费；其它 operate（pod/wf 等）无 reader，相当于 no-op。
	ctx = ContextWithOpVJPreTimings(ctx, queueGetMs, indexerGetMs)
	requeue, err := operate(ctx, obj, key, exists)
	if requeue {
		queue.AddAfter(key, time.Second)
	}
	if err != nil {
		logger.WithField("error", err).Error("Failed to operate workload")
		return true
	}

	logger.Infof("process workload finished, requeue: %t", requeue)
	return true
}

// operatePod处理任务中间态 queuing -> initializing -> running
func (ctrl *Controller) operatePod(ctx context.Context, obj interface{}, key string, exists bool) (bool, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return false, nil
	}

	round, _ := strconv.Atoi(pod.Annotations[annotationRetryRound])
	podInfo := &dao.PodInfo{
		JobID:      pod.Labels[labelJobID],
		PodName:    pod.Name,
		NodeName:   pod.Spec.NodeName,
		RetryRound: round,
	}
	if err := ctrl.db.InsertPodInfoIfNotExist(ctx, podInfo); err != nil {
		return true, errors.Wrap(err, "failed to insert pod info")
	}

	jobs, err := ctrl.db.GetJobStatus(ctx, map[string]interface{}{"job_id": pod.Labels[labelJobID]}, 1, 0)
	if err != nil {
		return true, errors.Wrap(err, "failed to get job status")
	}
	if len(jobs) == 0 {
		return true, fmt.Errorf("can not get job status")
	}
	job := jobs[0]

	needUpdate := false
	update := &dao.JobUpdateParams{
		ScheduledTime: ctrl.getRunningTime([]*v1.Pod{pod}),
		BeforePhase:   []string{pkg.JobPhaseInitializing, pkg.JobPhaseQueuing},
	}
	if pod.Status.StartTime != nil {
		update.ScheduledTime = pod.Status.StartTime.Time
	}
	if job.Phase == pkg.JobPhaseQueuing {
		if !update.ScheduledTime.IsZero() {
			needUpdate = true
			update.Phase = pkg.JobPhaseInitializing
		}
		if !update.RunningTime.IsZero() {
			needUpdate = true
			update.Phase = pkg.JobPhaseRunning
		}
	}
	if job.Phase == pkg.JobPhaseInitializing && !update.RunningTime.IsZero() {
		needUpdate = true
		update.Phase = pkg.JobPhaseRunning
	}

	if needUpdate {
		if err = ctrl.db.UpdateJobStatus(ctx, job.JobID, job.JobName, update); err != nil {
			return true, errors.Wrap(err, "failed to update job status")
		}
	} else if (job.ScheduledTime == nil && !update.ScheduledTime.IsZero()) || (job.RunningTime == nil && !update.RunningTime.IsZero()) {
		// Pod Infomer慢的情况下，会出现任务结束后，Pod事件才过来，尽量补全调度时间和运行时间
		if err = ctrl.db.UpdateJobStatus(ctx, job.JobID, job.JobName, &dao.JobUpdateParams{ScheduledTime: update.ScheduledTime, RunningTime: update.RunningTime}); err != nil {
			log.WithField("job", job.JobID).Warnf("Update job scheduledTime/runningTime fail: %v", err)
		}
	}

	return false, nil
}

func (ctrl *Controller) getRunningTime(pods []*v1.Pod) time.Time {
	for _, pod := range pods {
		if pod.Spec.NodeName == "" {
			continue
		}
		for _, v := range pod.Status.Conditions {
			if v.Type == v1.PodInitialized && v.Status == v1.ConditionTrue {
				return v.LastTransitionTime.Time
			}
		}
	}

	return time.Time{}
}

// startWorker 启动一个 worker goroutine，挂上 panic recover + WaitGroup join。
//
// 用同步版 wait.UntilWithContext 替代原 SafeUntilWithContext（后者内部 go 启动子
// goroutine，调用方立即返回，导致 join 失效）。同步版在 ctx cancel 后等本轮 fn 完成
// 再返回，wg.Done 真的发生在 worker 退出之后。
//
// 双层 defer recover：NewRecoverAndNotify 抓 panic 打日志 + 飞书通知，
// 外层兜底 recover 防止 notify.SendFS 在 test/dev 环境（如 nil notifier）触发二次 panic。
func (ctrl *Controller) startWorker(ctx context.Context, wg *sync.WaitGroup, fn func(ctx context.Context)) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Fatalf("startWorker recovered from panic: %v", r)
			}
		}()
		wait.UntilWithContext(ctx, fn, time.Second)
	}()
}

func (ctrl *Controller) initPodInformer() {
	// 1.添加Indexer回调
	err := ctrl.podInformer.Informer().AddIndexers(cache.Indexers{
		labelJobID: func(obj interface{}) ([]string, error) {
			pod := obj.(*v1.Pod)
			return []string{pod.Labels[labelJobID]}, nil
		},
		PodStatusIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return []string{}, nil
			}
			return []string{pod.Labels[PodStatusIndex]}, nil
		},
	})
	if err != nil {
		log.Fatalf("Can not add pod indexer %v", err)
	}

	// 2.添加Informer回调
	ctrl.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ctrl.enqueue(obj, ctrl.podQueue)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ctrl.enqueue(newObj, ctrl.podQueue)
		},
	})
}

// initEventInformer 初始化事件Informer的索引器
// 为Pod事件创建索引，便于快速查找
// 索引键为Pod名称，值为事件列表
func (ctrl *Controller) initEventInformer() {
	err := ctrl.evInformer.Informer().AddIndexers(cache.Indexers{
		PodEventIndex: func(obj interface{}) ([]string, error) {
			event, ok := obj.(*v1.Event)
			if !ok {
				log.Debug("Object is not an Event")
				return []string{}, nil
			}

			if event.InvolvedObject.Kind == "Pod" {
				log.WithFields(log.Fields{
					"pod":     event.InvolvedObject.Name,
					"reason":  event.Reason,
					"message": event.Message,
				}).Debug("Indexing pod event")
				return []string{event.InvolvedObject.Name}, nil
			}
			return []string{}, nil
		},
	})

	if err != nil {
		log.WithField("error", err).Fatal("Failed to add event indexer")
	}

	log.Info("Event informer indexer initialized successfully")
}

func (ctrl *Controller) enqueue(obj interface{}, q workqueue.RateLimitingInterface) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		log.Errorf("MetaNamespaceKeyFunc fail [%v]", err)
		return
	}
	q.Add(key)
}
