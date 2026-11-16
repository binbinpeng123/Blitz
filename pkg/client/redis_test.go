package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/binbinpeng123/blitz/pkg/conf"
	"github.com/go-redis/redis/v7"
	slardarlog "gitlab.hobot.cc/aidi/slardar/log"
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

func TestZSetClearAllMembers(t *testing.T) {
	ctx := context.Background()

	t.Run("removes every member", func(t *testing.T) {
		rc, mr := newTestRedisClient(t)
		for _, m := range []string{"a", "b", "c"} {
			if _, err := mr.ZAdd("queue", 1, m); err != nil {
				t.Fatalf("seed queue: %v", err)
			}
		}

		if err := rc.ZSetClearAllMembers(ctx, "queue"); err != nil {
			t.Fatalf("ZSetClearAllMembers: %v", err)
		}
		if mr.Exists("queue") {
			t.Fatal("key still exists after clear")
		}
	})

	t.Run("missing key is not an error", func(t *testing.T) {
		rc, _ := newTestRedisClient(t)
		if err := rc.ZSetClearAllMembers(ctx, "missing"); err != nil {
			t.Fatalf("ZSetClearAllMembers: %v", err)
		}
	})

	t.Run("redis error returned", func(t *testing.T) {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis.Run: %v", err)
		}
		rc := &RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})}
		mr.Close()

		if err := rc.ZSetClearAllMembers(ctx, "queue"); err == nil {
			t.Fatal("expected error after redis shutdown")
		}
	})
}

func TestZSetAddMulti(t *testing.T) {
	ctx := context.Background()

	t.Run("adds all members across batches", func(t *testing.T) {
		rc, mr := newTestRedisClient(t)
		// 超过一批（batchZAddSize）以覆盖分批写入逻辑
		const total = 2*batchZAddSize + 300
		items := make([]*redis.Z, 0, total)
		for i := 0; i < total; i++ {
			items = append(items, &redis.Z{Member: fmt.Sprintf("m%d", i), Score: float64(i)})
		}

		if err := rc.ZSetAddMulti(ctx, "big", items...); err != nil {
			t.Fatalf("ZSetAddMulti: %v", err)
		}
		members, err := mr.ZMembers("big")
		if err != nil {
			t.Fatalf("ZMembers: %v", err)
		}
		if got := len(members); got != total {
			t.Fatalf("member count = %d, want %d", got, total)
		}
		score, err := mr.ZScore("big", "m1234")
		if err != nil {
			t.Fatalf("member missing: %v", err)
		}
		if score != 1234 {
			t.Fatalf("score = %v, want 1234", score)
		}
	})

	t.Run("empty items is a no-op", func(t *testing.T) {
		rc, mr := newTestRedisClient(t)
		if err := rc.ZSetAddMulti(ctx, "empty"); err != nil {
			t.Fatalf("ZSetAddMulti: %v", err)
		}
		if mr.Exists("empty") {
			t.Fatal("key should not be created for empty items")
		}
	})

	t.Run("redis error returned", func(t *testing.T) {
		mr, err := miniredis.Run()
		if err != nil {
			t.Fatalf("miniredis.Run: %v", err)
		}
		rc := &RedisClient{Client: redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})}
		mr.Close()

		// 错误分支会通过 slardar log 打日志，先初始化避免 nil logger panic
		slardarlog.Init()

		items := []*redis.Z{{Member: "a", Score: 1}}
		if err := rc.ZSetAddMulti(ctx, "queue", items...); err == nil {
			t.Fatal("expected error after redis shutdown")
		}
	})
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
