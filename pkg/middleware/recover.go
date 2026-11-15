package middleware

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/binbinpeng123/blitz/pkg/notify"
	"gitlab.hobot.cc/aidi/slardar/env"
	"gitlab.hobot.cc/aidi/slardar/log"
	"k8s.io/apimachinery/pkg/util/wait"
)

// SafeUntilWithContext 封装wait.UntilWithContext，新增panic recover能力
func SafeUntilWithContext(ctx context.Context, name string, fn func(ctx context.Context), period time.Duration) {
	if period <= 0 {
		log.Get(ctx).Errorf("skip periodic task %s: period must be positive, got %s", name, period)
		return
	}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("[Recovery] %s \nenv: %s\napp_id: %s\ncron_func:%s\npanic recovered: Error:%s\nCall Stack:%s",
					time.Now(), env.GetDeployEnv(env.ServiceMode), env.ServiceName, name, r, debug.Stack())

				log.Get(ctx).Error(msg)
				notify.SendFS(ctx, msg)
			}
		}()
		fn(ctx)
	}, period)
}
