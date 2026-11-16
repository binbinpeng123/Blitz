package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/binbinpeng123/blitz/pkg"
	"github.com/binbinpeng123/blitz/pkg/conf"
	"github.com/binbinpeng123/blitz/pkg/dao"
	"github.com/binbinpeng123/blitz/pkg/middleware"
	"github.com/binbinpeng123/blitz/pkg/rank"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/labels"
	runtimeutil "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/set"
	volcanoapis "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

const (
	submitterIdleDelay       = time.Second
	submitterMaxErrorBackoff = 30 * time.Second
	chunkSize                = 100
)

var (
	lastQueueBucketTimestamp atomic.Int64

	RedisKeyQueuingJob      = "queue:job_submit_%s"
	RedisKeyQueuingJobLimit = "queue:job_limit_%s"
)

// 定时从db中取任务执行
func (ctrl *Controller) runCron(ctx context.Context) {
	// 定时刷新job rank
	go middleware.SafeUntilWithContext(ctx, "refreshJobRank", ctrl.refreshJobRank, time.Duration(conf.Conf.RefreshJobRankIntervalMinutes)*time.Minute)
	// 定时扫描db提交job
	go middleware.SafeUntilWithContext(ctx, "submitJobs", ctrl.submitJobs, time.Minute)
}

// 定时刷新job rank
func (ctrl *Controller) refreshJobRank(ctx context.Context) {
	log.Info("Start refresh job rank")

	nsObjs, _ := ctrl.nsListener.List(labels.Everything())
	var nss []string
	for _, v := range nsObjs {
		nss = append(nss, v.Name)
	}

	workqueue.ParallelizeUntil(ctx, 50, len(nss), func(index int) {
		ctrl.doRefreshRank(ctx, nss[index])
	})

}

func (ctrl *Controller) doRefreshRank(ctx context.Context, ns string) {
	// 获取job
	jobs, err := ctrl.db.GetJobStatus(ctx, map[string]interface{}{"phase__in": []string{pkg.JobPhasePending, pkg.JobPhaseQueuing}, "namespace": ns, "k8s": ctrl.k8s}, 0, 0)
	if err != nil {
		log.WithField("err", err).Error("Get jobs fail")
		return
	}
	if len(jobs) == 0 {
		log.WithField("namespace", ns).Info("No pending or queuing jobs found")
		return
	}

	// 重新给job排序
	if err = ctrl.rankController.InitJobRankQueue(ctx, ns, jobs); err != nil {
		log.WithFields(log.Fields{"err": err, "namespace": ns}).Error("sort jobs in redis fail, sort in cache now")
	}

	// 对job排序
	sort.Slice(jobs, func(i, j int) bool {
		// k8s提交时间为准
		if jobs[i].SubmittedTime != nil && jobs[j].SubmittedTime == nil {
			return true
		}
		if jobs[i].SubmittedTime == nil && jobs[j].SubmittedTime != nil {
			return false
		}
		// 高优任务优先
		if jobs[i].Priority != jobs[j].Priority {
			return pkg.JobPriorityRank(jobs[i].Priority) > pkg.JobPriorityRank(jobs[j].Priority)
		}
		// 用户提交时间前面的优先
		if !jobs[i].JobSubmitedTime.Equal(jobs[j].JobSubmitedTime) {
			return jobs[i].JobSubmitedTime.Before(jobs[j].JobSubmitedTime)
		}
		// jobId字典序
		return jobs[i].JobID < jobs[j].JobID
	})

	// 并发批量更新job在db中状态
	for i := range jobs {
		jobs[i].ColRank = i + 1
	}
	var wg sync.WaitGroup
	for i := 0; i < len(jobs); i += chunkSize {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			start := index
			end := index + chunkSize
			if end > len(jobs) {
				end = len(jobs)
			}
			if err = ctrl.db.BatchUpdateJobRank(ctx, jobs[start:end]); err != nil {
				log.WithField("err", err).Error("BatchUpdateJobRank fail")
				return
			}
		}(i)
	}
	wg.Wait()
}

// 提交jobs
func (ctrl *Controller) submitJobs(ctx context.Context) {
	nsObjs, _ := ctrl.nsListener.List(labels.Everything())
	nss := set.Set[string]{}
	for _, v := range nsObjs {
		nss.Insert(v.Name)
		if _, ok := ctrl.submitingNSsWithCancel[v.Name]; !ok {
			c, cancel := context.WithCancel(ctx)
			ctrl.submitingNSsWithCancel[v.Name] = cancel
			go middleware.SafeUntilWithContext(context.WithValue(c, "ns", v.Name), "startSubmitter", ctrl.startSubmitter, time.Second)
		}
	}
	if len(nss) == 0 {
		log.Info("no ns found, skip it")
		return
	}
	for ns, cancel := range ctrl.submitingNSsWithCancel {
		if !nss.Has(ns) {
			log.WithField("ns", ns).Info("ns deleted, Submitter canceled")
			cancel()
			delete(ctrl.submitingNSsWithCancel, ns)
		}
	}
}

func (ctrl *Controller) startSubmitter(ctx context.Context) {
	defer runtimeutil.HandleCrash()

	var delay time.Duration
	for {
		keepRunning, submitErr := ctrl.doSubmitJob(ctx)
		if !keepRunning {
			ns, _ := ctx.Value("ns").(string)
			log.WithField("ns", ns).Info("Submitter context canceled")
			return
		}

		delay = nextSubmitterDelay(delay, submitErr)
		if !waitSubmitter(ctx, delay) {
			ns := ctx.Value("ns").(string)
			log.WithField("ns", ns).Info("Submitter context canceled")
			return
		}
	}

}

func (ctrl *Controller) doSubmitJob(ctx context.Context) (isContinue bool, submitErr error) {
	ns := ctx.Value("ns").(string)
	isContinue = true
	l := log.WithField("ns", ns)
	l.Info("Start submitJobs")

	// 任务阻塞，拒绝提交任务
	if ctrl.isSubmitBlock(ctx, ns) {
		return false, nil
	}

	// 从redis取top n job执行
	where := map[string]interface{}{
		"namespace": ns,
		"k8s":       ctrl.k8s,
	}
	limit := 0
	jobIDs, err := ctrl.rankController.GetTopRankPendingJobs(ctx, ns, conf.Conf.SubmitJobNum)
	if err == nil && len(jobIDs) == 0 {
		l.Infof("no pending jobs in %s, sleeping 3 seconds", ns)
		time.Sleep(time.Second * 3)
		return
	}

	// 从redis执行失败，使用mysql兜底
	if err != nil && !pkg.IsRedisNilError(err) {
		l.WithError(err).Errorf("get pending jobs from redis fail,  will query db")
		where["col_rank__order_by"] = "asc"
		where["phase"] = pkg.JobPhasePending
		limit = int(conf.Conf.SubmitJobNum)
	} else {
		where["job_id__in"] = jobIDs
	}

	l.Infof("start get jobs from db, jobIDs: %v", jobIDs)
	jobs, err := ctrl.db.GetJobStatus(ctx, where, limit, 0)
	if err != nil {
		l.WithError(err).Errorf("GetJobstatus fail")
		return true, err
	}
	if len(jobs) == 0 {
		l.Infof("no pending jobs in %s, sleeping 3 seconds", ns)
		time.Sleep(time.Second * 3)
		return
	}
	l.Infof("get pending jobs from db, jobIDs: %v", jobIDs)

	// 并发执行创建vj job
	var wg sync.WaitGroup
	var firstErr error
	var firstErrMux sync.Mutex
	recordErr := func(err error) {
		if err == nil {
			return
		}
		firstErrMux.Lock()
		defer firstErrMux.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	wg.Add(len(jobs))
	for _, job := range jobs {
		go func(job *dao.JobStatus) {
			defer wg.Done()
			lj := log.WithField("job_id", job.JobID)

			if job.Phase != pkg.JobPhasePending {
				lj.Infof("job %s phase is not pending, remove it from created queue", job.JobID)
				ctrl.rankController.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, rank.RankEventCompensate)
				return
			}

			lj.Info("Start submit job")

			updateFailPhase, err := ctrl.createVJ(ctx, job)
			if err != nil {
				lj.WithError(err).Error("createVJ fail")
				recordErr(err)
				if updateFailPhase {
					update := &dao.JobUpdateParams{
						Phase:       pkg.JobPhaseFailed,
						BeforePhase: []string{pkg.JobPhasePending},
						Reason:      err.Error(),
						FinishTime:  time.Now(),
					}
					recordErr(ctrl.finishJob(ctx, job, update))
					return
				}
				return
			}

			data := &dao.JobUpdateParams{
				Phase:         pkg.JobPhaseQueuing,
				BeforePhase:   []string{pkg.JobPhasePending},
				SubmittedTime: time.Now(),
			}
			if job.SubmittedTime != nil {
				data.SubmittedTime = *job.SubmittedTime
			}
			// 更新数据库job状态
			if err = ctrl.db.UpdateJobStatus(ctx, job.JobID, job.JobName, data); err != nil {
				lj.WithError(err).Error("UpJobSubmitStatus fail")
				recordErr(err)
				return
			}
			// 更新redis排队状态
			ctrl.rankController.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, rank.RankEventSubmitted)

			lj.Info("Submit vj success")
		}(job)
	}
	wg.Wait()

	return true, firstErr
}

func (ctrl *Controller) isSubmitBlock(ctx context.Context, ns string) bool {
	maxPending := conf.Conf.MaximumQueuePendingJobs
	if maxPending <= 0 {
		maxPending = 1000
	}

	vjCnt := ctrl.getVJCount(ns, volcanoapis.Pending)

	// 每5分钟更新一次排队的任务数
	nowUnix := time.Now().Unix()
	lastUpdate := lastQueueBucketTimestamp.Load()
	if nowUnix-lastUpdate >= int64((5*time.Minute).Seconds()) && lastQueueBucketTimestamp.CompareAndSwap(lastUpdate, nowUnix) {
		ctrl.redisClient.SetIntValueByKey(ctx, fmt.Sprintf(RedisKeyQueuingJob, ns), int64(vjCnt))
		ctrl.redisClient.SetIntValueByKey(ctx, fmt.Sprintf(RedisKeyQueuingJobLimit, ns), int64(maxPending))
	}

	// informer中vjJob负载
	if vjCnt >= maxPending {
		log.WithFields(log.Fields{
			"ns":           ns,
			"cache_vj_cnt": vjCnt,
			"max_pending":  maxPending,
		}).Warning("Skip submitting due to queue block")
		ctrl.redisClient.SetIntValueByKey(ctx, fmt.Sprintf(RedisKeyQueuingJob, ns), int64(vjCnt))
		ctrl.redisClient.SetIntValueByKey(ctx, fmt.Sprintf(RedisKeyQueuingJobLimit, ns), int64(maxPending))
	}

	// redis中积压任务负载
	queueingJobsCount, err := ctrl.rankController.GetQueueingJobsCount(ctx, ns)
	if err != nil {
		return false
	}

	if int(queueingJobsCount) > maxPending {
		log.WithFields(log.Fields{
			"ns":                    ns,
			"redis_queuing_job_cnt": int(queueingJobsCount),
			"max_pending":           maxPending,
		}).Warning("Skip submitting due to queue block")
		return true
	}

	return false
}

func nextSubmitterDelay(current time.Duration, submitErr error) time.Duration {
	if submitErr != nil {
		return submitterIdleDelay
	}
	if current <= 0 {
		return submitterIdleDelay
	}
	if current >= submitterMaxErrorBackoff {
		return submitterMaxErrorBackoff
	}
	current *= 2
	if current > submitterMaxErrorBackoff {
		return submitterMaxErrorBackoff
	}

	return current
}

func waitSubmitter(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
