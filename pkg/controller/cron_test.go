package controller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/binbinpeng123/blitz/pkg/client"
	"github.com/binbinpeng123/blitz/pkg/conf"
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
