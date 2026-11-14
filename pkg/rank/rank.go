package rank

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/binbinpeng123/blitz/pkg"
	"github.com/binbinpeng123/blitz/pkg/client"
	"github.com/binbinpeng123/blitz/pkg/dao"
	log "github.com/sirupsen/logrus"
)

const (
	RankEventCreated    = "created"
	RankEventSubmitted  = "submitted"
	RankEventScheduled  = "scheduled"
	RankEventStopped    = "stopped"
	RankEventRetry      = "retry"
	RankEventCompensate = "compensate"
	RankEventUrgent     = "urgent"
)

const (
	createdJobRankQueue   string = "%s_created_job_rank_queue"
	submittedJobRankQueue string = "%s_submitted_job_rank_queue"
)

const (
	maxTimestamp   int64   = 4102444800
	base           float64 = 256.0
	maxUUIDScore   float64 = 2.5
	priorityWeight float64 = 1e10
	jobIDWeight    float64 = 1e-1
)

// job优先级 > 提交时间 > 随机字典序
func calculate(job *dao.JobStatus) float64 {
	priorityScore := float64(pkg.JobPriorityRank(job.Priority))
	submittedTimeScore := float64(maxTimestamp - job.SubmittedTime.Unix())
	jobNameScore := jobIDScoreFloat(job.JobID)

	return priorityScore*priorityWeight + submittedTimeScore + jobNameScore*jobIDWeight
}

func jobIDScoreFloat(jobID string) float64 {
	parts := strings.Split(jobID, "-")
	if len(parts) < 2 {
		return 0
	}
	uuidPart := parts[len(parts)-1]
	var score float64
	for i := 0; i < len(uuidPart); i++ {
		score += float64(uuidPart[i]) / math.Pow(base, float64(i+1))
	}

	return maxUUIDScore - score
}

type RankController struct {
	redisClient *client.RedisClient
}

func NewRankController() *RankController {
	return &RankController{redisClient: client.NewRedisClient()}
}

// 通过mysql状态更新redis
func (ctrl *RankController) UpdateJobRankQueue(ctx context.Context, jobs []*dao.JobStatus, rankEvent string) {
	for _, job := range jobs {
		if job == nil && rankEvent == "" {
			continue
		}
		namespace := job.Namespace
		jobID := job.JobID

		createdKey := fmt.Sprintf(createdJobRankQueue, namespace)
		submittedKey := fmt.Sprintf(submittedJobRankQueue, namespace)
		switch rankEvent {
		// 任务重试或创建
		case RankEventRetry, RankEventCreated:
			score := calculate(job)
			if err := pkg.Retry(3, func() error {
				return ctrl.redisClient.ZSetAdd(ctx, createdKey, jobID, score)
			}); err != nil {
				log.WithField("err", err).Errorf("Failed to add job %s to created job rank queue", jobID)
			}
		// 任务优先级提升
		case RankEventUrgent:
			score := calculate(job)
			if job.Phase == pkg.JobPhasePending {
				if err := pkg.Retry(3, func() error {
					return ctrl.redisClient.ZSetAdd(ctx, createdKey, jobID, score)
				}); err != nil {
					log.WithField("err", err).Errorf("Failed to add job %s to created job rank queue", jobID)
				}
			} else if job.Phase == pkg.JobPhaseQueuing {
				if err := pkg.Retry(3, func() error {
					return ctrl.redisClient.ZSetAdd(ctx, submittedKey, jobID, score)
				}); err != nil {
					log.WithField("err", err).Errorf("Failed to add job %s from created to submitted queue", jobID)
				}
			}
		// 任务已提交
		case RankEventSubmitted:
			score := calculate(job)
			if err := pkg.Retry(3, func() error {
				return ctrl.redisClient.ZSetMoveWithScore(ctx, createdKey, submittedKey, jobID, score)
			}); err != nil {
				log.WithField("err", err).Errorf("Failed to move job %s from created to submitted queue", jobID)
				return
			}
		// 任务已调度
		case RankEventScheduled:
			if err := pkg.Retry(3, func() error {
				return ctrl.redisClient.ZSetRemove(ctx, submittedKey, jobID)
			}); err != nil {
				log.WithField("err", err).Errorf("Failed to remove job %s from submitted queue", jobID)
			}
		// 任务已停止：提交过从提交队列移除，没提交过从创建队列移除
		case RankEventStopped:
			if job.SubmittedTime == nil {
				if err := pkg.Retry(3, func() error {
					return ctrl.redisClient.ZSetRemove(ctx, createdKey, jobID)
				}); err != nil {
					log.WithField("err", err).Errorf("Failed to remove job %s from created queue", jobID)
				}
			} else {
				if err := pkg.Retry(3, func() error {
					return ctrl.redisClient.ZSetRemove(ctx, submittedKey, jobID)
				}); err != nil {
					log.WithField("err", err).Errorf("Failed to remove job %s from submitted queue", jobID)
				}
			}
		case RankEventCompensate:
			if err := pkg.Retry(3, func() error {
				return ctrl.redisClient.ZSetRemove(ctx, createdKey, jobID)
			}); err != nil {
				log.WithField("err", err).Errorf("Failed to remove job %s from created queue", jobID)
			}
		}
	}
}

// get top n pending jobID
func (ctrl *RankController) GetTopRankPendingJobs(ctx context.Context, namespace string, n int64) (jobIDs []string, err error) {
	topN, err := ctrl.redisClient.ZSetTopN(ctx, fmt.Sprintf(createdJobRankQueue, namespace), n)
	if err != nil && !pkg.IsRedisNilError(err) {
		log.WithField("err", err).Errorf("Failed to get top %d jobs in created queue for namespace %s", n, namespace)
		return nil, err
	}
	for _, job := range topN {
		jobID, ok := job.Member.(string)
		if !ok {
			log.WithField("namespace", namespace).Errorf("Expected job.Member to be string, got %T, jobID: %v", job.Member, job.Member)
			continue
		}
		jobIDs = append(jobIDs, jobID)
	}
	log.WithField("namespace", namespace).Infof("Find top rank pending jobs %v", jobIDs)
	return jobIDs, nil
}

func (ctrl *RankController) GetQueueingJobsCount(ctx context.Context, namespace string) (count int64, err error) {
	return ctrl.redisClient.ZSetCount(ctx, fmt.Sprintf(submittedJobRankQueue, namespace))
}
