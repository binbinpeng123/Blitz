package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/binbinpeng123/blitz/pkg/conf"
	"gitlab.hobot.cc/aidi/slardar/log"
)

// initTestLog 初始化 slardar 日志；SafeUntilWithContext 的 recover 分支会调用
// log.Get(ctx)，未初始化时 logger 为 nil 会二次 panic。
func initTestLog() {
	log.Init()
}

func TestSafeUntilWithContextNonPositivePeriod(t *testing.T) {
	initTestLog()

	for _, period := range []time.Duration{0, -time.Second} {
		var called atomic.Bool
		SafeUntilWithContext(context.Background(), "skip", func(context.Context) {
			called.Store(true)
		}, period)
		// 非正 period 直接返回，不启动 goroutine
		time.Sleep(20 * time.Millisecond)
		if called.Load() {
			t.Fatalf("fn called with period %s, want skipped", period)
		}
	}
}

func TestSafeUntilWithContextRecoversAndContinues(t *testing.T) {
	initTestLog()

	// recover 分支会调用 notify.SendFS，这里用 httptest 接住飞书请求，
	// 避免 conf 中 URL 为空导致 http.Client.Do(nil) panic。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	conf.Conf = conf.Config{FeishuRobotURL: srv.URL}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	continued := make(chan struct{}, 1)
	SafeUntilWithContext(ctx, "test", func(context.Context) {
		n := calls.Add(1)
		// 前两次 panic，验证恢复后仍继续执行
		if n <= 2 {
			panic("boom")
		}
		select {
		case continued <- struct{}{}:
		default:
		}
	}, 10*time.Millisecond)

	select {
	case <-continued:
	case <-time.After(3 * time.Second):
		t.Fatalf("fn stopped after panic, calls=%d", calls.Load())
	}
	if got := calls.Load(); got < 3 {
		t.Fatalf("calls = %d, want >= 3", got)
	}
}

func TestSafeUntilWithContextStopsOnCancel(t *testing.T) {
	initTestLog()

	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	SafeUntilWithContext(ctx, "stop", func(context.Context) {
		calls.Add(1)
	}, 5*time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() == 0 {
		t.Fatal("fn never called")
	}

	cancel()
	time.Sleep(30 * time.Millisecond)
	before := calls.Load()
	time.Sleep(50 * time.Millisecond)
	// 取消瞬间可能还有一次在途调用，允许 +1
	if got := calls.Load(); got > before+1 {
		t.Fatalf("fn still running after cancel: before=%d after=%d", before, got)
	}
}
