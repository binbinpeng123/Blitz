package rank

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/binbinpeng123/blitz/pkg"
	"github.com/binbinpeng123/blitz/pkg/client"
	"github.com/binbinpeng123/blitz/pkg/dao"
	"github.com/go-redis/redis/v7"
)

func newTestRankController(t *testing.T) (*RankController, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return &RankController{
		redisClient: &client.RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
	}, mr
}

func testJob(id, ns, phase, priority string, submitted *time.Time) *dao.JobStatus {
	return &dao.JobStatus{
		JobID:         id,
		Namespace:     ns,
		Phase:         phase,
		Priority:      priority,
		SubmittedTime: submitted,
	}
}

func TestCalculate(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := base.Add(time.Second)

	t.Run("priority dominates", func(t *testing.T) {
		low := calculate(testJob("job-a", "default", pkg.JobPhasePending, pkg.Low, &base))
		highest := calculate(testJob("job-a", "default", pkg.JobPhasePending, pkg.Highest, &later))
		if highest <= low {
			t.Fatalf("highest priority score %v should be > low priority score %v", highest, low)
		}
	})

	t.Run("earlier submitted time ranks higher", func(t *testing.T) {
		earlier := calculate(testJob("job-a", "default", pkg.JobPhasePending, pkg.High, &base))
		late := calculate(testJob("job-a", "default", pkg.JobPhasePending, pkg.High, &later))
		if earlier <= late {
			t.Fatalf("earlier score %v should be > later score %v", earlier, late)
		}
	})

	t.Run("job id breaks tie lexicographically", func(t *testing.T) {
		a := calculate(testJob("job-a", "default", pkg.JobPhasePending, pkg.High, &base))
		b := calculate(testJob("job-b", "default", pkg.JobPhasePending, pkg.High, &base))
		if a <= b {
			t.Fatalf("job-a score %v should be > job-b score %v", a, b)
		}
	})

	t.Run("unknown priority treated as low", func(t *testing.T) {
		unknown := calculate(testJob("job-a", "default", pkg.JobPhasePending, "unknown", &base))
		low := calculate(testJob("job-a", "default", pkg.JobPhasePending, pkg.Low, &base))
		if math.Abs(unknown-low) > 1e-9 {
			t.Fatalf("unknown priority score %v != low priority score %v", unknown, low)
		}
	})
}

func TestJobIDScoreFloat(t *testing.T) {
	if got := jobIDScoreFloat("nodash"); got != 0 {
		t.Fatalf("jobIDScoreFloat(nodash) = %v, want 0", got)
	}
	if got := jobIDScoreFloat("job-"); got != maxUUIDScore {
		t.Fatalf("jobIDScoreFloat(job-) = %v, want %v", got, maxUUIDScore)
	}
	a := jobIDScoreFloat("job-a")
	b := jobIDScoreFloat("job-b")
	if a <= b {
		t.Fatalf("job-a score %v should be > job-b score %v", a, b)
	}
	// 分数落在 (0, maxUUIDScore] 区间内
	if a <= 0 || a > maxUUIDScore {
		t.Fatalf("score %v out of range (0, %v]", a, maxUUIDScore)
	}
}

func TestUpdateJobRankQueue(t *testing.T) {
	ctx := context.Background()
	const (
		createdKey   = "default_created_job_rank_queue"
		submittedKey = "default_submitted_job_rank_queue"
	)

	submittedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("created adds to created queue with calculated score", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhasePending, pkg.High, &submittedAt)

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventCreated)

		score, err := mr.ZScore(createdKey, "j1")
		if err != nil {
			t.Fatalf("member missing: %v", err)
		}
		if math.Abs(score-calculate(job)) > 1e-9 {
			t.Fatalf("score = %v, want %v", score, calculate(job))
		}
	})

	t.Run("retry behaves like created", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhasePending, pkg.Medium, &submittedAt)

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventRetry)

		if _, err := mr.ZScore(createdKey, "j1"); err != nil {
			t.Fatalf("member missing in created queue: %v", err)
		}
	})

	t.Run("urgent pending goes to created queue", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhasePending, pkg.Urgent, &submittedAt)

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventUrgent)

		if _, err := mr.ZScore(createdKey, "j1"); err != nil {
			t.Fatalf("member missing in created queue: %v", err)
		}
		if _, err := mr.ZScore(submittedKey, "j1"); err == nil {
			t.Fatal("member should not be in submitted queue")
		}
	})

	t.Run("urgent queuing goes to submitted queue", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhaseQueuing, pkg.Urgent, &submittedAt)

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventUrgent)

		if _, err := mr.ZScore(submittedKey, "j1"); err != nil {
			t.Fatalf("member missing in submitted queue: %v", err)
		}
	})

	t.Run("submitted moves member between queues", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhaseQueuing, pkg.High, &submittedAt)
		if _, err := mr.ZAdd(createdKey, 1, "j1"); err != nil {
			t.Fatalf("seed created queue: %v", err)
		}

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventSubmitted)

		if _, err := mr.ZScore(createdKey, "j1"); err == nil {
			t.Fatal("member still in created queue")
		}
		score, err := mr.ZScore(submittedKey, "j1")
		if err != nil {
			t.Fatalf("member missing in submitted queue: %v", err)
		}
		if math.Abs(score-calculate(job)) > 1e-9 {
			t.Fatalf("score = %v, want %v", score, calculate(job))
		}
	})

	t.Run("scheduled removes from submitted queue", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhaseRunning, pkg.High, &submittedAt)
		if _, err := mr.ZAdd(submittedKey, 1, "j1"); err != nil {
			t.Fatalf("seed submitted queue: %v", err)
		}

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventScheduled)

		if _, err := mr.ZScore(submittedKey, "j1"); err == nil {
			t.Fatal("member still in submitted queue")
		}
	})

	t.Run("stopped before submit removes from created queue", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhaseFailed, pkg.High, nil)
		if _, err := mr.ZAdd(createdKey, 1, "j1"); err != nil {
			t.Fatalf("seed created queue: %v", err)
		}

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventStopped)

		if _, err := mr.ZScore(createdKey, "j1"); err == nil {
			t.Fatal("member still in created queue")
		}
	})

	t.Run("stopped after submit removes from submitted queue", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhaseFailed, pkg.High, &submittedAt)
		if _, err := mr.ZAdd(submittedKey, 1, "j1"); err != nil {
			t.Fatalf("seed submitted queue: %v", err)
		}

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventStopped)

		if _, err := mr.ZScore(submittedKey, "j1"); err == nil {
			t.Fatal("member still in submitted queue")
		}
	})

	t.Run("compensate removes from created queue", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		job := testJob("j1", "default", pkg.JobPhaseQueuing, pkg.High, &submittedAt)
		if _, err := mr.ZAdd(createdKey, 1, "j1"); err != nil {
			t.Fatalf("seed created queue: %v", err)
		}

		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventCompensate)

		if _, err := mr.ZScore(createdKey, "j1"); err == nil {
			t.Fatal("member still in created queue")
		}
	})

	t.Run("nil job with empty event is skipped", func(t *testing.T) {
		rc, _ := newTestRankController(t)
		// 不应 panic
		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{nil}, "")
	})

	t.Run("redis failure does not panic", func(t *testing.T) {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis.Run: %v", err)
		}
		rc := &RankController{
			redisClient: &client.RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})},
		}
		mr.Close()

		job := testJob("j1", "default", pkg.JobPhasePending, pkg.High, &submittedAt)
		rc.UpdateJobRankQueue(ctx, []*dao.JobStatus{job}, RankEventCreated)
	})
}

func TestGetTopRankPendingJobs(t *testing.T) {
	ctx := context.Background()
	const createdKey = "default_created_job_rank_queue"

	t.Run("returns top n in descending score order", func(t *testing.T) {
		rc, mr := newTestRankController(t)
		for member, score := range map[string]float64{"a": 1, "b": 5, "c": 3} {
			if _, err := mr.ZAdd(createdKey, score, member); err != nil {
				t.Fatalf("seed queue: %v", err)
			}
		}

		ids, err := rc.GetTopRankPendingJobs(ctx, "default", 2)
		if err != nil {
			t.Fatalf("GetTopRankPendingJobs: %v", err)
		}
		if strings.Join(ids, ",") != "b,c" {
			t.Fatalf("ids = %v, want [b c]", ids)
		}
	})

	t.Run("empty queue returns no ids", func(t *testing.T) {
		rc, _ := newTestRankController(t)
		ids, err := rc.GetTopRankPendingJobs(ctx, "default", 5)
		if err != nil {
			t.Fatalf("GetTopRankPendingJobs: %v", err)
		}
		if len(ids) != 0 {
			t.Fatalf("ids = %v, want empty", ids)
		}
	})

	t.Run("redis error returned", func(t *testing.T) {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis.Run: %v", err)
		}
		rc := &RankController{
			redisClient: &client.RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})},
		}
		mr.Close()

		if _, err := rc.GetTopRankPendingJobs(ctx, "default", 5); err == nil {
			t.Fatal("expected error after redis shutdown")
		}
	})
}

func TestGetQueueingJobsCount(t *testing.T) {
	ctx := context.Background()
	const submittedKey = "default_submitted_job_rank_queue"

	rc, mr := newTestRankController(t)
	for _, m := range []string{"a", "b"} {
		if _, err := mr.ZAdd(submittedKey, 1, m); err != nil {
			t.Fatalf("seed queue: %v", err)
		}
	}

	count, err := rc.GetQueueingJobsCount(ctx, "default")
	if err != nil {
		t.Fatalf("GetQueueingJobsCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}
