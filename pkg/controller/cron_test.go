package controller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/binbinpeng123/blitz/pkg"
	"github.com/binbinpeng123/blitz/pkg/client"
	"github.com/binbinpeng123/blitz/pkg/conf"
	"github.com/binbinpeng123/blitz/pkg/dao"
	"github.com/binbinpeng123/blitz/pkg/rank"
	"github.com/go-redis/redis/v7"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

func TestNextSubmitterDelay(t *testing.T) {
	tests := []struct {
		name    string
		current time.Duration
		err     error
		want    time.Duration
	}{
		{"error resets to idle", 10 * time.Second, errors.New("boom"), submitterIdleDelay},
		{"zero current", 0, nil, submitterIdleDelay},
		{"negative current", -time.Second, nil, submitterIdleDelay},
		{"idle doubles", submitterIdleDelay, nil, 2 * time.Second},
		{"four seconds doubles", 4 * time.Second, nil, 8 * time.Second},
		{"clamped by max", 20 * time.Second, nil, submitterMaxErrorBackoff},
		{"already max", submitterMaxErrorBackoff, nil, submitterMaxErrorBackoff},
		{"exactly doubles to max", 15 * time.Second, nil, submitterMaxErrorBackoff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextSubmitterDelay(tt.current, tt.err); got != tt.want {
				t.Fatalf("nextSubmitterDelay(%v, %v) = %v, want %v", tt.current, tt.err, got, tt.want)
			}
		})
	}
}

func TestWaitSubmitter(t *testing.T) {
	t.Run("returns true after delay", func(t *testing.T) {
		if !waitSubmitter(context.Background(), 10*time.Millisecond) {
			t.Fatal("waitSubmitter() = false, want true")
		}
	})

	t.Run("returns false when ctx canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		if waitSubmitter(ctx, time.Hour) {
			t.Fatal("waitSubmitter() = true, want false")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("waitSubmitter blocked %v after cancel", elapsed)
		}
	})
}

func newNamespaceLister(t *testing.T, names ...string) listersv1.NamespaceLister {
	t.Helper()
	idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, name := range names {
		if err := idx.Add(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
			t.Fatalf("indexer.Add(%s): %v", name, err)
		}
	}
	return listersv1.NewNamespaceLister(idx)
}

func TestSubmitJobs(t *testing.T) {
	t.Run("no namespace returns early", func(t *testing.T) {
		ctrl := &Controller{
			nsListener:             newNamespaceLister(t),
			submitingNSsWithCancel: map[string]context.CancelFunc{},
		}

		ctrl.submitJobs(context.Background())

		if len(ctrl.submitingNSsWithCancel) != 0 {
			t.Fatalf("submitters = %d, want 0", len(ctrl.submitingNSsWithCancel))
		}
	})

	t.Run("existing submitter is kept", func(t *testing.T) {
		var canceled atomic.Bool
		ctrl := &Controller{
			nsListener: newNamespaceLister(t, "default"),
			submitingNSsWithCancel: map[string]context.CancelFunc{
				"default": func() { canceled.Store(true) },
			},
		}

		ctrl.submitJobs(context.Background())

		if canceled.Load() {
			t.Fatal("existing submitter was canceled, want kept")
		}
		if len(ctrl.submitingNSsWithCancel) != 1 {
			t.Fatalf("submitters = %d, want 1", len(ctrl.submitingNSsWithCancel))
		}
	})
}

func newCronController(t *testing.T, maxPending int) (*Controller, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	conf.Conf = conf.Config{
		RedisConfig:             &conf.Redis{MasterAddr: mr.Addr()},
		MaximumQueuePendingJobs: maxPending,
		SubmitJobNum:            1,
	}
	vjInformer, _ := newFakeVJInformer(t)
	return &Controller{
		redisClient:            &client.RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
		rankController:         rank.NewRankController(),
		vjInformer:             vjInformer,
		submitingNSsWithCancel: map[string]context.CancelFunc{},
	}, mr
}

func TestIsSubmitBlock(t *testing.T) {
	const submittedKey = "default_submitted_job_rank_queue"

	t.Run("queue below limit is not blocked", func(t *testing.T) {
		ctrl, mr := newCronController(t, 2)
		if _, err := mr.ZAdd(submittedKey, 1, "j1"); err != nil {
			t.Fatalf("seed queue: %v", err)
		}
		lastQueueBucketTimestamp.Store(time.Now().Unix())
		if ctrl.isSubmitBlock(context.Background(), "default") {
			t.Fatal("isSubmitBlock() = true, want false")
		}
	})

	t.Run("queue equal to limit is not blocked", func(t *testing.T) {
		ctrl, mr := newCronController(t, 2)
		for _, m := range []string{"j1", "j2"} {
			if _, err := mr.ZAdd(submittedKey, 1, m); err != nil {
				t.Fatalf("seed queue: %v", err)
			}
		}
		lastQueueBucketTimestamp.Store(time.Now().Unix())
		if ctrl.isSubmitBlock(context.Background(), "default") {
			t.Fatal("isSubmitBlock() = true, want false")
		}
	})

	t.Run("queue above limit is blocked", func(t *testing.T) {
		ctrl, mr := newCronController(t, 2)
		for _, m := range []string{"j1", "j2", "j3"} {
			if _, err := mr.ZAdd(submittedKey, 1, m); err != nil {
				t.Fatalf("seed queue: %v", err)
			}
		}
		lastQueueBucketTimestamp.Store(time.Now().Unix())
		if !ctrl.isSubmitBlock(context.Background(), "default") {
			t.Fatal("isSubmitBlock() = false, want true")
		}
	})

	t.Run("non-positive config falls back to default", func(t *testing.T) {
		ctrl, mr := newCronController(t, 0)
		if _, err := mr.ZAdd(submittedKey, 1, "j1"); err != nil {
			t.Fatalf("seed queue: %v", err)
		}
		lastQueueBucketTimestamp.Store(time.Now().Unix())
		if ctrl.isSubmitBlock(context.Background(), "default") {
			t.Fatal("isSubmitBlock() = true, want false with default limit")
		}
	})

	t.Run("redis error is not blocked", func(t *testing.T) {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis.Run: %v", err)
		}
		conf.Conf = conf.Config{
			RedisConfig:             &conf.Redis{MasterAddr: mr.Addr()},
			MaximumQueuePendingJobs: 2,
		}
		vjInformer, _ := newFakeVJInformer(t)
		ctrl := &Controller{
			redisClient:    &client.RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
			rankController: rank.NewRankController(),
			vjInformer:     vjInformer,
		}
		// 关闭 redis 后 rankController 查询会失败；同时把 5 分钟刷新分支跳过，
		// 避免 SetIntValueByKey 失败日志依赖 slardar logger 初始化。
		lastQueueBucketTimestamp.Store(time.Now().Unix())
		mr.Close()

		if ctrl.isSubmitBlock(context.Background(), "default") {
			t.Fatal("isSubmitBlock() = true, want false on redis error")
		}
	})
}

// newRefreshController 构造 rank 刷新用例的 Controller，db 由 newMockDao 提供。
func newRefreshController(t *testing.T, db *dao.Dao, namespaces ...string) (*Controller, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	conf.Conf = conf.Config{RedisConfig: &conf.Redis{MasterAddr: mr.Addr()}}
	return &Controller{
		db:             db,
		k8s:            "c1",
		nsListener:     newNamespaceLister(t, namespaces...),
		rankController: rank.NewRankController(),
		redisClient:    &client.RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})},
	}, mr
}

func submittedValue(job *dao.JobStatus) interface{} {
	if job.SubmittedTime == nil {
		return nil
	}
	return *job.SubmittedTime
}

// jobStatusRows 只返回 doRefreshRank 用到的列，sqlx 按列名映射。
func jobStatusRows(jobs ...*dao.JobStatus) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"id", "job_id", "job_name", "phase", "priority",
		"job_submited_time", "submitted_time", "col_rank", "namespace", "k8s",
	})
	for _, job := range jobs {
		rows.AddRow(job.ID, job.JobID, job.JobName, job.Phase, job.Priority,
			job.JobSubmitedTime, submittedValue(job), job.ColRank, job.Namespace, job.K8S)
	}
	return rows
}

func TestDoRefreshRank(t *testing.T) {
	ctx := context.Background()
	const submittedKey = "default_submitted_job_rank_queue"
	selectSQL := `select * from job_status|phase in ("Pending","Queuing")|namespace = "default"|k8s = "c1"|is_deleted = 0|last_record = 0`

	t.Run("sorts jobs and writes ranks", func(t *testing.T) {
		db, mock := newMockDao(t)
		ctrl, mr := newRefreshController(t, db, "default")

		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		submitted1 := base.Add(-time.Minute)
		submitted2 := base.Add(-90 * time.Second)
		submitted3 := base.Add(-30 * time.Second)
		jobs := []*dao.JobStatus{
			{ID: 1, JobID: "job-aaa-001", Phase: pkg.JobPhaseQueuing, Priority: pkg.High, JobSubmitedTime: base.Add(-100 * time.Second), SubmittedTime: &submitted1, Namespace: "default", K8S: "c1"},
			// 高优任务排在高优之前
			{ID: 2, JobID: "job-bbb-002", Phase: pkg.JobPhaseQueuing, Priority: pkg.Urgent, JobSubmitedTime: base.Add(-300 * time.Second), SubmittedTime: &submitted2, Namespace: "default", K8S: "c1"},
			// 低优任务排最后
			{ID: 3, JobID: "job-ccc-003", Phase: pkg.JobPhaseQueuing, Priority: pkg.Low, JobSubmitedTime: base.Add(-50 * time.Second), SubmittedTime: &submitted3, Namespace: "default", K8S: "c1"},
		}
		mock.ExpectQuery(selectSQL).WillReturnRows(jobStatusRows(jobs...))
		mock.ExpectExec("UPDATE job_status SET col_rank = CASE|" +
			"WHEN id = 2 THEN 1|WHEN id = 1 THEN 2|WHEN id = 3 THEN 3|id IN (2,1,3)").
			WillReturnResult(sqlmock.NewResult(0, 3))

		ctrl.doRefreshRank(ctx, "default")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
		// submitted 队列按分数从高到低：urgent > high > low
		high, err := mr.ZScore(submittedKey, "job-aaa-001")
		if err != nil {
			t.Fatalf("job-aaa-001 missing in submitted queue: %v", err)
		}
		urgent, err := mr.ZScore(submittedKey, "job-bbb-002")
		if err != nil {
			t.Fatalf("job-bbb-002 missing in submitted queue: %v", err)
		}
		low, err := mr.ZScore(submittedKey, "job-ccc-003")
		if err != nil {
			t.Fatalf("job-ccc-003 missing in submitted queue: %v", err)
		}
		if !(urgent > high && high > low) {
			t.Fatalf("scores not ordered by priority: urgent=%v high=%v low=%v", urgent, high, low)
		}
		if mr.Exists("default_created_job_rank_queue") {
			t.Fatal("created queue should stay empty when no pending jobs")
		}
	})

	t.Run("no jobs skips redis and db update", func(t *testing.T) {
		db, mock := newMockDao(t)
		ctrl, mr := newRefreshController(t, db, "default")

		mock.ExpectQuery(selectSQL).WillReturnRows(jobStatusRows())

		ctrl.doRefreshRank(ctx, "default")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
		if len(mr.Keys()) != 0 {
			t.Fatalf("unexpected redis keys: %v", mr.Keys())
		}
	})

	t.Run("db error returns without touching redis", func(t *testing.T) {
		db, mock := newMockDao(t)
		ctrl, mr := newRefreshController(t, db, "default")

		mock.ExpectQuery(selectSQL).WillReturnError(errors.New("db down"))

		ctrl.doRefreshRank(ctx, "default")

		if len(mr.Keys()) != 0 {
			t.Fatalf("unexpected redis keys: %v", mr.Keys())
		}
	})

	t.Run("redis failure still writes ranks to db", func(t *testing.T) {
		db, mock := newMockDao(t)
		ctrl, mr := newRefreshController(t, db, "default")

		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		job := &dao.JobStatus{ID: 1, JobID: "job-aaa-001", Phase: pkg.JobPhaseQueuing, Priority: pkg.High, JobSubmitedTime: base, SubmittedTime: &base, Namespace: "default", K8S: "c1"}
		mock.ExpectQuery(selectSQL).WillReturnRows(jobStatusRows(job))
		mock.ExpectExec("UPDATE job_status SET col_rank = CASE|WHEN id = 1 THEN 1").
			WillReturnResult(sqlmock.NewResult(0, 1))

		// 关闭 redis，InitJobRankQueue 失败后仍应回落到内存排序 + 写库
		mr.Close()

		ctrl.doRefreshRank(ctx, "default")

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestRefreshJobRank(t *testing.T) {
	ctx := context.Background()
	db, mock := newMockDao(t)
	ctrl, _ := newRefreshController(t, db, "default")

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	submitted := base.Add(time.Minute)
	job := &dao.JobStatus{ID: 1, JobID: "job-aaa-001", Phase: pkg.JobPhaseQueuing, Priority: pkg.High, JobSubmitedTime: base, SubmittedTime: &submitted, Namespace: "default", K8S: "c1"}
	mock.ExpectQuery(`select * from job_status|phase in ("Pending","Queuing")|namespace = "default"|k8s = "c1"`).
		WillReturnRows(jobStatusRows(job))
	mock.ExpectExec(`UPDATE job_status SET col_rank = CASE|WHEN id = 1 THEN 1`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	ctrl.refreshJobRank(ctx)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
