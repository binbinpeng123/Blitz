package controller

import "context"

type ctxKeyOpVJPreTimings struct{}

// opVJPreTimings 承载 processNextItem 在 operateVJ 调用之前测得的耗时。
type opVJPreTimings struct {
	queueGetMs   int64
	indexerGetMs int64
}

// ContextWithOpVJPreTimings 把预测得的 timing 挂到 ctx。
// 即使开关关闭也会注入：operateVJ 入口 tracker==nil 时自然不会消费，相当于 no-op。
func ContextWithOpVJPreTimings(ctx context.Context, queueGetMs, indexerGetMs int64) context.Context {
	return context.WithValue(ctx, ctxKeyOpVJPreTimings{}, opVJPreTimings{
		queueGetMs:   queueGetMs,
		indexerGetMs: indexerGetMs,
	})
}
