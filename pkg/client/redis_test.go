package client

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/binbinpeng123/blitz/pkg/conf"
	"github.com/go-redis/redis/v7"
)

func newTestRedisClient(t *testing.T) (*RedisClient, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	conf.Conf = conf.Config{RedisConfig: &conf.Redis{MasterAddr: mr.Addr()}}
	return NewRedisClient(), mr
}

func TestNewRedisClient(t *testing.T) {
	t.Run("ping ok", func(t *testing.T) {
		rc, mr := newTestRedisClient(t)
		if rc.Client == nil {
			t.Fatal("Client is nil")
		}
		if err := rc.Client.Ping().Err(); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		if mr.Addr() == "" {
			t.Fatal("miniredis addr is empty")
		}
	})

	t.Run("ping failed panics", func(t *testing.T) {
		// 127.0.0.1:1 上没有服务，连接会被拒绝
		conf.Conf = conf.Config{RedisConfig: &conf.Redis{MasterAddr: "127.0.0.1:1"}}
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("NewRedisClient did not panic on unreachable redis")
			}
		}()
		NewRedisClient()
	})
}

func TestZSetAddAndTopN(t *testing.T) {
	ctx := context.Background()
	rc, _ := newTestRedisClient(t)

	if err := rc.ZSetAdd(ctx, "rank", "a", 1); err != nil {
		t.Fatalf("ZSetAdd(a): %v", err)
	}
	if err := rc.ZSetAdd(ctx, "rank", "b", 5); err != nil {
		t.Fatalf("ZSetAdd(b): %v", err)
	}
	if err := rc.ZSetAdd(ctx, "rank", "c", 3); err != nil {
		t.Fatalf("ZSetAdd(c): %v", err)
	}

	top, err := rc.ZSetTopN(ctx, "rank", 2)
	if err != nil {
		t.Fatalf("ZSetTopN: %v", err)
	}
	if len(top) != 2 {
		t.Fatalf("got %d members, want 2", len(top))
	}
	// 按分数从高到低
	if top[0].Member != "b" || top[1].Member != "c" {
		t.Fatalf("unexpected order: %+v", top)
	}
	if top[0].Score != 5 {
		t.Fatalf("top[0].Score = %v, want 5", top[0].Score)
	}

	all, err := rc.ZSetTopN(ctx, "rank", 10)
	if err != nil {
		t.Fatalf("ZSetTopN(all): %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("got %d members, want 3", len(all))
	}
}

func TestZSetTopNEmptyKey(t *testing.T) {
	rc, _ := newTestRedisClient(t)
	all, err := rc.ZSetTopN(context.Background(), "missing", 5)
	if err != nil {
		t.Fatalf("ZSetTopN: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("got %d members, want 0", len(all))
	}
}

func TestZSetMoveWithScore(t *testing.T) {
	ctx := context.Background()
	rc, mr := newTestRedisClient(t)

	if err := rc.ZSetAdd(ctx, "from", "j1", 1); err != nil {
		t.Fatalf("ZSetAdd: %v", err)
	}
	if err := rc.ZSetMoveWithScore(ctx, "from", "to", "j1", 2.5); err != nil {
		t.Fatalf("ZSetMoveWithScore: %v", err)
	}
	if _, err := mr.ZScore("from", "j1"); err == nil {
		t.Fatal("member still exists in from queue")
	}
	score, err := mr.ZScore("to", "j1")
	if err != nil {
		t.Fatalf("member missing in to queue: %v", err)
	}
	if score != 2.5 {
		t.Fatalf("score = %v, want 2.5", score)
	}
}

func TestZSetRemoveAndCount(t *testing.T) {
	ctx := context.Background()
	rc, _ := newTestRedisClient(t)

	for _, m := range []string{"a", "b", "c"} {
		if err := rc.ZSetAdd(ctx, "queue", m, 1); err != nil {
			t.Fatalf("ZSetAdd(%s): %v", m, err)
		}
	}
	if n, err := rc.ZSetCount(ctx, "queue"); err != nil || n != 3 {
		t.Fatalf("ZSetCount = %d, %v, want 3, nil", n, err)
	}

	if err := rc.ZSetRemove(ctx, "queue", "b"); err != nil {
		t.Fatalf("ZSetRemove: %v", err)
	}
	if n, err := rc.ZSetCount(ctx, "queue"); err != nil || n != 2 {
		t.Fatalf("ZSetCount = %d, %v, want 2, nil", n, err)
	}
}

func TestSetIntValueByKey(t *testing.T) {
	ctx := context.Background()
	rc, _ := newTestRedisClient(t)

	if err := rc.SetIntValueByKey(ctx, "counter", 42); err != nil {
		t.Fatalf("SetIntValueByKey: %v", err)
	}
	got, err := rc.Client.Get("counter").Int64()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestRedisErrorPaths(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	rc := &RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})}
	mr.Close()

	ctx := context.Background()
	if err := rc.ZSetAdd(ctx, "k", "a", 1); err == nil {
		t.Fatal("ZSetAdd: expected error after redis shutdown")
	}
	if _, err := rc.ZSetTopN(ctx, "k", 1); err == nil {
		t.Fatal("ZSetTopN: expected error after redis shutdown")
	}
	if _, err := rc.ZSetCount(ctx, "k"); err == nil {
		t.Fatal("ZSetCount: expected error after redis shutdown")
	}
}
