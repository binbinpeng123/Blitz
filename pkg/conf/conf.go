package conf

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
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
