package controller

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/binbinpeng123/blitz/pkg"
	"github.com/binbinpeng123/blitz/pkg/dao"
	jmoironsqlx "github.com/jmoiron/sqlx"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	slardarsqlx "gitlab.hobot.cc/aidi/slardar/sqlx"
)

// containsMatcher 用 "|" 分隔期望片段，命中全部片段即通过。
func containsMatcher(expectedSQL, actualSQL string) error {
	normActual := strings.Join(strings.Fields(actualSQL), " ")
	for _, fragment := range strings.Split(expectedSQL, "|") {
		normWant := strings.Join(strings.Fields(fragment), " ")
		if !strings.Contains(normActual, normWant) {
			return &sqlMismatchError{actual: normActual, want: normWant}
		}
	}
	return nil
}

type sqlMismatchError struct {
	actual string
	want   string
}

func (e *sqlMismatchError) Error() string {
	return "sql " + e.actual + " does not contain " + e.want
}

func newMockDao(t *testing.T) (*dao.Dao, sqlmock.Sqlmock) {
	t.Helper()
	sdb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(containsMatcher)))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = sdb.Close() })
	return &dao.Dao{DB: &slardarsqlx.DB{DB: jmoironsqlx.NewDb(sdb, "mysql").Unsafe()}}, mock
}

func defaultPod(start time.Time) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod-1",
			Namespace:   "default",
			Labels:      map[string]string{labelJobID: "j1"},
			Annotations: map[string]string{annotationRetryRound: "2"},
		},
		Spec: v1.PodSpec{NodeName: "node-1"},
		Status: v1.PodStatus{
			StartTime: &metav1.Time{Time: start},
			Conditions: []v1.PodCondition{{
				Type:               v1.PodInitialized,
				Status:             v1.ConditionTrue,
				LastTransitionTime: metav1.Time{Time: start.Add(time.Second)},
			}},
		},
	}
}

func expectInsertPodInfo(mock sqlmock.Sqlmock) {
	mock.ExpectExec("INSERT IGNORE INTO pod_info|job_id, pod_name, node_name, retry_round").
		WithArgs("j1", "pod-1", "node-1", 2, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

// expectSelectJob 返回 id/job_id/job_name/phase/scheduled_time/running_time 六列。
func expectSelectJob(mock sqlmock.Sqlmock, rows ...driver.Value) {
	r := sqlmock.NewRows([]string{"id", "job_id", "job_name", "phase", "scheduled_time", "running_time"})
	if len(rows) > 0 {
		r.AddRow(rows...)
	}
	mock.ExpectQuery("select * from job_status|job_id = \"j1\"|is_deleted = 0|last_record = 0").WillReturnRows(r)
}

func TestGetRunningTime(t *testing.T) {
	transition := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		pods []*v1.Pod
		want time.Time
	}{
		{
			name: "no pods",
			pods: nil,
			want: time.Time{},
		},
		{
			name: "pod not scheduled",
			pods: []*v1.Pod{{
				Status: v1.PodStatus{Conditions: []v1.PodCondition{{
					Type: v1.PodInitialized, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition},
				}}},
			}},
			want: time.Time{},
		},
		{
			name: "pod initialized",
			pods: []*v1.Pod{{
				Spec: v1.PodSpec{NodeName: "node-1"},
				Status: v1.PodStatus{Conditions: []v1.PodCondition{{
					Type: v1.PodInitialized, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition},
				}}},
			}},
			want: transition,
		},
		{
			name: "pod initialized false",
			pods: []*v1.Pod{{
				Spec: v1.PodSpec{NodeName: "node-1"},
				Status: v1.PodStatus{Conditions: []v1.PodCondition{{
					Type: v1.PodInitialized, Status: v1.ConditionFalse, LastTransitionTime: metav1.Time{Time: transition},
				}}},
			}},
			want: time.Time{},
		},
		{
			name: "skip unscheduled pod and pick next",
			pods: []*v1.Pod{
				{Status: v1.PodStatus{Conditions: []v1.PodCondition{{
					Type: v1.PodInitialized, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition.Add(-time.Minute)},
				}}}},
				{
					Spec: v1.PodSpec{NodeName: "node-2"},
					Status: v1.PodStatus{Conditions: []v1.PodCondition{{
						Type: v1.PodInitialized, Status: v1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition},
					}}},
				},
			},
			want: transition,
		},
	}

	ctrl := &Controller{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ctrl.getRunningTime(tt.pods)
			if !got.Equal(tt.want) {
				t.Fatalf("getRunningTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnqueue(t *testing.T) {
	ctrl := &Controller{}

	t.Run("pod enqueued by namespace key", func(t *testing.T) {
		q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
		defer q.ShutDown()

		ctrl.enqueue(defaultPod(time.Now()), q)
		item, quit := q.Get()
		if quit {
			t.Fatal("queue unexpectedly shut down")
		}
		defer q.Done(item)
		if item != "default/pod-1" {
			t.Fatalf("key = %v, want default/pod-1", item)
		}
	})

	t.Run("invalid object is dropped", func(t *testing.T) {
		q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
		defer q.ShutDown()

		ctrl.enqueue("not-a-k8s-object", q)
		if q.Len() != 0 {
			t.Fatalf("queue len = %d, want 0", q.Len())
		}
	})
}

func TestProcessNextItem(t *testing.T) {
	newIndexer := func(t *testing.T) cache.Indexer {
		idx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		if err := idx.Add(defaultPod(time.Now())); err != nil {
			t.Fatalf("indexer.Add: %v", err)
		}
		return idx
	}

	t.Run("found object and timings injected", func(t *testing.T) {
		q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
		defer q.ShutDown()
		q.Add("default/pod-1")

		var (
			gotObj    interface{}
			gotKey    string
			gotExists bool
			gotTiming opVJPreTimings
			gotOK     bool
		)
		operate := func(ctx context.Context, obj interface{}, key string, exists bool) (bool, error) {
			gotObj, gotKey, gotExists = obj, key, exists
			gotTiming, gotOK = ctx.Value(ctxKeyOpVJPreTimings{}).(opVJPreTimings)
			return false, nil
		}

		ctrl := &Controller{}
		if ok := ctrl.processNextItem(context.Background(), 0, q, newIndexer(t), operate); !ok {
			t.Fatal("processNextItem returned false, want true")
		}
		if gotObj == nil || gotKey != "default/pod-1" || !gotExists {
			t.Fatalf("unexpected operate args: obj=%v key=%q exists=%t", gotObj, gotKey, gotExists)
		}
		if !gotOK {
			t.Fatal("timings not injected into ctx")
		}
		if gotTiming.queueGetMs < 0 || gotTiming.indexerGetMs < 0 {
			t.Fatalf("unexpected timings: %+v", gotTiming)
		}
	})

	t.Run("missing key passes exists=false", func(t *testing.T) {
		q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
		defer q.ShutDown()
		q.Add("default/missing")

		var gotExists bool
		var called bool
		operate := func(_ context.Context, obj interface{}, _ string, exists bool) (bool, error) {
			called = true
			gotExists = exists
			if obj != nil {
				t.Fatalf("obj = %v, want nil", obj)
			}
			return false, nil
		}

		ctrl := &Controller{}
		ctrl.processNextItem(context.Background(), 0, q, cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}), operate)
		if !called {
			t.Fatal("operate not called")
		}
		if gotExists {
			t.Fatal("exists = true, want false")
		}
	})

	t.Run("requeue adds item back", func(t *testing.T) {
		q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
		defer q.ShutDown()
		q.Add("default/pod-1")

		operate := func(context.Context, interface{}, string, bool) (bool, error) {
			return true, nil
		}
		ctrl := &Controller{}
		ctrl.processNextItem(context.Background(), 0, q, newIndexer(t), operate)
		// AddAfter 有延迟，立即检查队列为空即可证明没有同步重复入队
		if q.Len() != 0 {
			t.Fatalf("queue len = %d, want 0", q.Len())
		}
	})

	t.Run("shutdown queue stops processing", func(t *testing.T) {
		q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
		q.ShutDown()

		called := false
		operate := func(context.Context, interface{}, string, bool) (bool, error) {
			called = true
			return false, nil
		}
		ctrl := &Controller{}
		if ok := ctrl.processNextItem(context.Background(), 0, q, newIndexer(t), operate); ok {
			t.Fatal("processNextItem returned true after shutdown")
		}
		if called {
			t.Fatal("operate should not be called after shutdown")
		}
	})
}

func TestOperatePod(t *testing.T) {
	start := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)

	t.Run("non pod object ignored", func(t *testing.T) {
		ctrl := &Controller{}
		requeue, err := ctrl.operatePod(context.Background(), "not-a-pod", "key", true)
		if requeue || err != nil {
			t.Fatalf("got requeue=%t err=%v, want false/nil", requeue, err)
		}
	})

	t.Run("queuing job advances to initializing", func(t *testing.T) {
		db, mock := newMockDao(t)
		expectInsertPodInfo(mock)
		expectSelectJob(mock, 1, "j1", "job-name", pkg.JobPhaseQueuing, nil, nil)
		mock.ExpectExec("update job_status set phase = ?,col_rank = 0,scheduled_time = ?|where is_deleted = false").
			WithArgs(pkg.JobPhaseInitializing, sqlmock.AnyArg(), sqlmock.AnyArg(), "j1",
				pkg.JobPhaseInitializing, pkg.JobPhaseQueuing).
			WillReturnResult(sqlmock.NewResult(0, 0))

		ctrl := &Controller{db: db}
		requeue, err := ctrl.operatePod(context.Background(), defaultPod(start), "default/pod-1", true)
		if requeue || err != nil {
			t.Fatalf("got requeue=%t err=%v, want false/nil", requeue, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("initializing job backfills scheduled time", func(t *testing.T) {
		db, mock := newMockDao(t)
		expectInsertPodInfo(mock)
		expectSelectJob(mock, 1, "j1", "job-name", pkg.JobPhaseInitializing, nil, nil)
		mock.ExpectExec("update job_status set scheduled_time = ?").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), "j1").
			WillReturnResult(sqlmock.NewResult(0, 1))

		ctrl := &Controller{db: db}
		requeue, err := ctrl.operatePod(context.Background(), defaultPod(start), "default/pod-1", true)
		if requeue || err != nil {
			t.Fatalf("got requeue=%t err=%v, want false/nil", requeue, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("running job with times set needs no update", func(t *testing.T) {
		db, mock := newMockDao(t)
		expectInsertPodInfo(mock)
		expectSelectJob(mock, 1, "j1", "job-name", pkg.JobPhaseRunning, start, start)

		ctrl := &Controller{db: db}
		requeue, err := ctrl.operatePod(context.Background(), defaultPod(start), "default/pod-1", true)
		if requeue || err != nil {
			t.Fatalf("got requeue=%t err=%v, want false/nil", requeue, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("job not found requeues", func(t *testing.T) {
		db, mock := newMockDao(t)
		expectInsertPodInfo(mock)
		expectSelectJob(mock)

		ctrl := &Controller{db: db}
		requeue, err := ctrl.operatePod(context.Background(), defaultPod(start), "default/pod-1", true)
		if !requeue {
			t.Fatal("requeue = false, want true")
		}
		if err == nil || !strings.Contains(err.Error(), "can not get job status") {
			t.Fatalf("err = %v, want can not get job status", err)
		}
	})

	t.Run("insert pod info error requeues", func(t *testing.T) {
		db, mock := newMockDao(t)
		mock.ExpectExec("INSERT IGNORE INTO pod_info").WillReturnError(errTest)

		ctrl := &Controller{db: db}
		requeue, err := ctrl.operatePod(context.Background(), defaultPod(start), "default/pod-1", true)
		if !requeue {
			t.Fatal("requeue = false, want true")
		}
		if err == nil || !strings.Contains(err.Error(), "failed to insert pod info") {
			t.Fatalf("err = %v, want failed to insert pod info", err)
		}
	})
}

type testError string

func (e testError) Error() string { return string(e) }

const errTest = testError("db down")
