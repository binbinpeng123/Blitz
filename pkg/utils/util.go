package utils

import (
	"github.com/binbinpeng123/blitz/pkg/conf"
	v1 "k8s.io/api/core/v1"
)

type BuildJobExtraData struct {
	JobOutput  conf.Dataset
	ClusterTmp conf.Dataset
	BucketPvc  map[string]string

	// OffloadEnv 是否需要将环境变量从Pod中抽离出来放到ConfigMap
	OffloadEnv bool
	// OffloadConfigName 抽离环境变量的ConfigMap名称
	OffloadConfigMap *v1.ConfigMap
}
