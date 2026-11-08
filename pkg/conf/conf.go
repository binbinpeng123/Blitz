package conf

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultUserMaxWorkflowCount           = 10000
	DefaultDBMaxOpenConns                 = 32
	DefaultDBMaxIdleConns                 = 8
	DefaultDBConnMaxLifetimeSeconds       = 30 * 60
	DefaultDBConnMaxIdleTimeSeconds       = 2 * 60
	DefaultRefreshJobRankIntervalMinutes  = 5
	DefaultRefreshRetryDagIntervalSeconds = 30

	// dedup cache 默认覆盖弹性短任务 + 长训练（≤15d）画像。
	DefaultBillingDedupCacheCapacity = 1_000_000      // ~200MB 内存占用
	DefaultBillingDedupTTLSeconds    = 15 * 24 * 3600 // 15 天

	// producer flush 默认 10ms：低延迟优先，安全调整范围 5–100ms。
	DefaultBillingProducerFlushFrequencyMs = 10

	// ReportFault 重试配置默认值
	DefaultReportFaultMaxRetries        = 3 // 最大重试次数
	DefaultReportFaultRetryIntervalSecs = 3 // 重试间隔秒数
)

var Conf Config

// Load 从 yaml 文件解析配置并覆盖全局 Conf。
func Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}
	Conf = cfg
	return nil
}

type Config struct {
	K8SKubeconfig                  map[string]string `yaml:"k8s_kubeconfig"`
	DBDsn                          map[string]string `yaml:"db_dsn"`
	DBMaxOpenConns                 int               `yaml:"db_max_open_conns"`
	DBMaxIdleConns                 int               `yaml:"db_max_idle_conns"`
	DBConnMaxLifetimeSeconds       int               `yaml:"db_conn_max_lifetime_seconds"`
	DBConnMaxIdleTimeSeconds       int               `yaml:"db_conn_max_idle_time_seconds"`
	RefreshJobRankIntervalMinutes  int               `yaml:"refresh_job_rank_interval_minutes"`
	RefreshRetryDagIntervalSeconds int               `yaml:"refresh_retry_dag_interval_seconds"`

	// 老 Kafka 集群，pkg/client/kafka.go 使用
	KafkaServers string `yaml:"kafka_servers"`
}

func (f *Config) Init() {
	if f.DBMaxOpenConns == 0 {
		f.DBMaxOpenConns = DefaultDBMaxOpenConns
	}
	if f.DBMaxIdleConns == 0 {
		f.DBMaxIdleConns = DefaultDBMaxIdleConns
	}
	if f.DBConnMaxLifetimeSeconds == 0 {
		f.DBConnMaxLifetimeSeconds = DefaultDBConnMaxLifetimeSeconds
	}
	if f.DBConnMaxIdleTimeSeconds == 0 {
		f.DBConnMaxIdleTimeSeconds = DefaultDBConnMaxIdleTimeSeconds
	}

	if f.RefreshJobRankIntervalMinutes == 0 {
		f.RefreshJobRankIntervalMinutes = DefaultRefreshJobRankIntervalMinutes
	}
	if f.RefreshRetryDagIntervalSeconds == 0 {
		f.RefreshRetryDagIntervalSeconds = DefaultRefreshRetryDagIntervalSeconds
	}
}

// ValidateDatabase validates database and periodic-task configuration after defaults are applied.
func (f *Config) ValidateDatabase() error {
	if len(f.K8SKubeconfig) == 0 {
		return fmt.Errorf("k8s config empty")
	}
	for cluster := range f.K8SKubeconfig {
		if strings.TrimSpace(f.DBDsn[cluster]) == "" {
			return fmt.Errorf("db_dsn missing for cluster %s", cluster)
		}
	}
	if f.DBMaxOpenConns <= 0 {
		return fmt.Errorf("db_max_open_conns must be positive, got %d", f.DBMaxOpenConns)
	}
	if f.DBMaxIdleConns < 0 {
		return fmt.Errorf("db_max_idle_conns must not be negative, got %d", f.DBMaxIdleConns)
	}
	if f.DBMaxIdleConns > f.DBMaxOpenConns {
		return fmt.Errorf("db_max_idle_conns %d exceeds db_max_open_conns %d", f.DBMaxIdleConns, f.DBMaxOpenConns)
	}
	if f.DBConnMaxLifetimeSeconds <= 0 {
		return fmt.Errorf("db_conn_max_lifetime_seconds must be positive, got %d", f.DBConnMaxLifetimeSeconds)
	}
	if f.DBConnMaxIdleTimeSeconds <= 0 {
		return fmt.Errorf("db_conn_max_idle_time_seconds must be positive, got %d", f.DBConnMaxIdleTimeSeconds)
	}
	if f.RefreshJobRankIntervalMinutes <= 0 {
		return fmt.Errorf("refresh_job_rank_interval_minutes must be positive, got %d", f.RefreshJobRankIntervalMinutes)
	}
	if f.RefreshRetryDagIntervalSeconds <= 0 {
		return fmt.Errorf("refresh_retry_dag_interval_seconds must be positive, got %d", f.RefreshRetryDagIntervalSeconds)
	}
	return nil
}

func (f *Config) Check() {
	f.Init()
	if err := f.ValidateDatabase(); err != nil {
		panic(err)
	}
}
