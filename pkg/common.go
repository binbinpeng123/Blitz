package pkg

import (
	"strings"
	"time"
)

func Retry(times int, f func() error) error {
	var err error
	for i := 0; i < times; i++ {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	return err
}

func IsRedisNilError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "redis: nil")
}
