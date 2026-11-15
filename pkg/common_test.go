package pkg

import (
	"errors"
	"strings"
	"testing"
)

func TestRetry(t *testing.T) {
	sentinel := errors.New("boom")

	t.Run("success on first try", func(t *testing.T) {
		calls := 0
		err := Retry(3, func() error {
			calls++
			return nil
		})
		if err != nil {
			t.Fatalf("Retry() = %v, want nil", err)
		}
		if calls != 1 {
			t.Fatalf("calls = %d, want 1", calls)
		}
	})

	t.Run("success after failures", func(t *testing.T) {
		calls := 0
		err := Retry(3, func() error {
			calls++
			if calls < 3 {
				return sentinel
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Retry() = %v, want nil", err)
		}
		if calls != 3 {
			t.Fatalf("calls = %d, want 3", calls)
		}
	})

	t.Run("exhausted retries returns last error", func(t *testing.T) {
		calls := 0
		err := Retry(2, func() error {
			calls++
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("Retry() = %v, want %v", err, sentinel)
		}
		if calls != 2 {
			t.Fatalf("calls = %d, want 2", calls)
		}
	})
}

func TestIsRedisNilError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"redis nil", errors.New("redis: nil"), true},
		{"wrapped redis nil", errors.New("get key: redis: nil"), true},
		{"other error", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRedisNilError(tt.err); got != tt.want {
				t.Fatalf("IsRedisNilError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}

	// 字符串匹配实现的边界：只包含前缀不算 nil 错误
	if IsRedisNilError(errors.New(strings.ToUpper("redis: nil"))) {
		t.Fatal("uppercase message should not match")
	}
}
