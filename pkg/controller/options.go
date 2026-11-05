package controller

import (
	"errors"
	"time"

	"k8s.io/client-go/tools/clientcmd"
)

// Options Controller进程的启动参数
type Options struct {
	ConfigFile   string
	ClientConfig clientcmd.ClientConfig
	Workers      int
	Resync       time.Duration
	Burst        int
	Qps          float32
	K8s          string
	// --loglevel
	LogLevel string
	// --gloglevel
	GLogLevel int
	// --log-format
	LogFormat string
}

// DefaultOptions 返回启动参数默认值
func DefaultOptions() *Options {
	return &Options{
		Workers: 1,
		Resync:  time.Second,
	}
}

func (opts *Options) Validate() error {
	if opts.Workers <= 0 {
		return errors.New("workers must be greater than zero")
	}
	if opts.Resync <= 0 {
		return errors.New("resync must be greater than zero")
	}
	return nil
}
