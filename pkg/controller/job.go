package controller

import (
	"context"
	"fmt"

	"github.com/binbinpeng123/blitz/pkg"
	"github.com/binbinpeng123/blitz/pkg/conf"
	"github.com/binbinpeng123/blitz/pkg/dao"
	"github.com/binbinpeng123/blitz/pkg/utils"
	"github.com/binbinpeng123/blitz/pkg/workload"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/util/retry"
	volcanoapis "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

func (ctrl *Controller) getVJCount(ns string, phase volcanoapis.JobPhase) int {
	vjs, _ := ctrl.vjInformer.Informer().GetIndexer().ByIndex(vjStatusIndex, fmt.Sprintf("%s/%s", ns, phase))
	return len(vjs)
}

func (ctrl *Controller) createVJ(ctx context.Context, job *dao.JobStatus) (updateFailPhase bool, err error) {
	// 获取job
	jobDatas, err := ctrl.db.GetJobData(ctx, map[string]interface{}{"job_id": job.JobID})
	if err != nil {
		return false, errors.Wrap(err, "Get JobData Failed")
	}
	if len(jobDatas) != 1 {
		return false, errors.Wrap(err, "Job Data not exist")
	}
	jobData := jobDatas[0]

	// 构建job的额外数据结构（存储）
	extraData, updateFailPhase, err := ctrl.buildJobExtraData(ctx, job, jobData)
	if err != nil {
		return updateFailPhase, errors.Wrap(err, "Build Job Extra Data")
	}

	// 数据处理任务将环境变量抽取出来放到ConfigMap
	if conf.Conf.OffloadJobEnv && job.JobType == pkg.DataProcessJob {
		extraData.OffloadEnv = true
		extraData.OffloadConfigMap = ctrl.buildJobEnvConfigMap(job)
	}

	// 创建vj
	vj, err := workload.BuildVolcanoJob(ctx, job, jobData, extraData)
	if err != nil {
		return false, err
	}

	// 提交vj
	// NOTE: 正常情况下应该先创建cm，再创建vj，最后设置cm的ownerReference指向vj。
	// 这里是为了降低QPS，以及cm创建后vj未创建而任务结束导致cm残留的场景，避免结束时的清理逻辑过于复杂
	// 影响：vj创建后，cm未创建而pod已调度，pod会卡在启动中，直到cm创建成功后才会继续启动，增加了任务的启动时间；这个问题概率较低，并且createVJ失败会持续重试，所以总体影响可控。
	var createdVJ *volcanoapis.Job
	if err = retry.OnError(retry.DefaultRetry, net.IsProbableEOF, func() error {
		createdVJ, err = ctrl.volcanoClient.BatchV1alpha1().Jobs(job.Namespace).Create(ctx, vj, metav1.CreateOptions{})
		return err
	}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return false, errors.Wrap(err, "Create VJ Failed")
		}
		createdVJ, _ = ctrl.vjInformer.Lister().Jobs(job.Namespace).Get(job.JobID)
		if createdVJ == nil {
			return false, errors.Wrap(err, "Get VJ Job Failed")
		}
		job.SubmittedTime = &createdVJ.CreationTimestamp.Time
	}

	if extraData.OffloadEnv {
		// NOTE: 在被序列化为强类型时，TypeMeta会被清空，需要手动补上以满足OwnerReference的要求
		createdVJ.TypeMeta = vj.TypeMeta
		setOwnerReference(extraData.OffloadConfigMap, createdVJ)
		if _, err = ctrl.kubeClient.CoreV1().ConfigMaps(job.Namespace).Create(ctx, extraData.OffloadConfigMap, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return false, errors.Wrap(err, "Create offload ConfigMap Failed")
		}
	}

	return false, nil
}

func (ctrl *Controller) buildJobExtraData(ctx context.Context, jobStatus *dao.JobStatus, jobData *dao.JobData) (utils.BuildJobExtraData, bool, error) {
	return utils.BuildJobExtraData{}, false, nil
}

func (ctrl *Controller) buildJobEnvConfigMap(job *dao.JobStatus) *corev1.ConfigMap {
	return &corev1.ConfigMap{}
}

func setOwnerReference(object metav1.Object, vj *volcanoapis.Job) {}
