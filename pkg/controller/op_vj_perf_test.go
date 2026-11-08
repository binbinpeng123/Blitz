package controller

import (
	"context"
	"testing"
)

func TestContextWithOpVJPreTimings(t *testing.T) {
	ctx := ContextWithOpVJPreTimings(context.Background(), 12, 34)

	timings, ok := ctx.Value(ctxKeyOpVJPreTimings{}).(opVJPreTimings)
	if !ok {
		t.Fatal("timings not found in context")
	}
	if timings.queueGetMs != 12 || timings.indexerGetMs != 34 {
		t.Fatalf("unexpected timings: %+v", timings)
	}

	if err := ctx.Err(); err != nil {
		t.Fatalf("context already done: %v", err)
	}
}
