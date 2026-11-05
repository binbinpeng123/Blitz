package conf

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() Config {
	return Config{
		K8SKubeconfig:                  map[string]string{"c1": "/path/c1", "c2": "/path/c2"},
		DBDsn:                          map[string]string{"c1": "root:root@tcp(127.0.0.1:3306)/c1", "c2": "root:root@tcp(127.0.0.1:3306)/c2"},
		DBMaxOpenConns:                 30,
		DBMaxIdleConns:                 10,
		DBConnMaxLifetimeSeconds:       1800,
		DBConnMaxIdleTimeSeconds:       600,
		RefreshJobRankIntervalMinutes:  5,
		RefreshRetryDagIntervalSeconds: 60,
	}
}

func TestValidateDatabase(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "valid", mutate: func(*Config) {}},
		{
			name:    "empty k8s config",
			mutate:  func(c *Config) { c.K8SKubeconfig = nil },
			wantErr: "k8s config empty",
		},
		{
			name: "missing dsn for cluster",
			mutate: func(c *Config) {
				delete(c.DBDsn, "c2")
			},
			wantErr: "db_dsn missing for cluster c2",
		},
		{
			name: "blank dsn for cluster",
			mutate: func(c *Config) {
				c.DBDsn["c1"] = "   "
			},
			wantErr: "db_dsn missing for cluster c1",
		},
		{
			name:    "non positive max open conns",
			mutate:  func(c *Config) { c.DBMaxOpenConns = 0 },
			wantErr: "db_max_open_conns must be positive",
		},
		{
			name:    "negative max idle conns",
			mutate:  func(c *Config) { c.DBMaxIdleConns = -1 },
			wantErr: "db_max_idle_conns must not be negative",
		},
		{
			name:    "idle exceeds open",
			mutate:  func(c *Config) { c.DBMaxIdleConns = c.DBMaxOpenConns + 1 },
			wantErr: "exceeds db_max_open_conns",
		},
		{
			name:    "non positive conn max lifetime",
			mutate:  func(c *Config) { c.DBConnMaxLifetimeSeconds = 0 },
			wantErr: "db_conn_max_lifetime_seconds must be positive",
		},
		{
			name:    "non positive conn max idle time",
			mutate:  func(c *Config) { c.DBConnMaxIdleTimeSeconds = 0 },
			wantErr: "db_conn_max_idle_time_seconds must be positive",
		},
		{
			name:    "non positive refresh job rank interval",
			mutate:  func(c *Config) { c.RefreshJobRankIntervalMinutes = 0 },
			wantErr: "refresh_job_rank_interval_minutes must be positive",
		},
		{
			name:    "non positive refresh retry dag interval",
			mutate:  func(c *Config) { c.RefreshRetryDagIntervalSeconds = 0 },
			wantErr: "refresh_retry_dag_interval_seconds must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)

			err := cfg.ValidateDatabase()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateDatabase() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateDatabase() = nil, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateDatabase() = %q, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	old := Conf
	t.Cleanup(func() { Conf = old })

	path := filepath.Join(t.TempDir(), "blitz.yaml")
	content := `
k8s_kubeconfig:
  c1: /path/c1
db_dsn:
  c1: root:root@tcp(127.0.0.1:3306)/blitz
db_max_open_conns: 30
db_max_idle_conns: 10
db_conn_max_lifetime_seconds: 1800
db_conn_max_idle_time_seconds: 600
refresh_job_rank_interval_minutes: 5
refresh_retry_dag_interval_seconds: 60
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := Load(path); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if got := Conf.K8SKubeconfig["c1"]; got != "/path/c1" {
		t.Fatalf("K8SKubeconfig[c1] = %q, want %q", got, "/path/c1")
	}
	if got := Conf.DBDsn["c1"]; got != "root:root@tcp(127.0.0.1:3306)/blitz" {
		t.Fatalf("DBDsn[c1] = %q, want dsn", got)
	}
	if Conf.DBMaxOpenConns != 30 || Conf.DBMaxIdleConns != 10 {
		t.Fatalf("pool config = (%d, %d), want (30, 10)", Conf.DBMaxOpenConns, Conf.DBMaxIdleConns)
	}
}

func TestLoadErrors(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing file", func(t *testing.T) {
		err := Load(filepath.Join(dir, "not-exist.yaml"))
		if err == nil || !strings.Contains(err.Error(), "read config file") {
			t.Fatalf("Load() = %v, want read config error", err)
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		path := filepath.Join(dir, "bad.yaml")
		if err := os.WriteFile(path, []byte("k8s_kubeconfig: ["), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "parse config file") {
			t.Fatalf("Load() = %v, want parse config error", err)
		}
	})
}

func TestInitAppliesDefaults(t *testing.T) {
	var cfg Config
	cfg.Init()

	if cfg.DBMaxOpenConns != DefaultDBMaxOpenConns {
		t.Fatalf("DBMaxOpenConns = %d, want %d", cfg.DBMaxOpenConns, DefaultDBMaxOpenConns)
	}
	if cfg.DBMaxIdleConns != DefaultDBMaxIdleConns {
		t.Fatalf("DBMaxIdleConns = %d, want %d", cfg.DBMaxIdleConns, DefaultDBMaxIdleConns)
	}
	if cfg.DBConnMaxLifetimeSeconds != DefaultDBConnMaxLifetimeSeconds {
		t.Fatalf("DBConnMaxLifetimeSeconds = %d, want %d", cfg.DBConnMaxLifetimeSeconds, DefaultDBConnMaxLifetimeSeconds)
	}
	if cfg.DBConnMaxIdleTimeSeconds != DefaultDBConnMaxIdleTimeSeconds {
		t.Fatalf("DBConnMaxIdleTimeSeconds = %d, want %d", cfg.DBConnMaxIdleTimeSeconds, DefaultDBConnMaxIdleTimeSeconds)
	}
	if cfg.RefreshJobRankIntervalMinutes != DefaultRefreshJobRankIntervalMinutes {
		t.Fatalf("RefreshJobRankIntervalMinutes = %d, want %d", cfg.RefreshJobRankIntervalMinutes, DefaultRefreshJobRankIntervalMinutes)
	}
	if cfg.RefreshRetryDagIntervalSeconds != DefaultRefreshRetryDagIntervalSeconds {
		t.Fatalf("RefreshRetryDagIntervalSeconds = %d, want %d", cfg.RefreshRetryDagIntervalSeconds, DefaultRefreshRetryDagIntervalSeconds)
	}
}

func TestInitKeepsExplicitValues(t *testing.T) {
	cfg := Config{
		DBMaxOpenConns:                 77,
		DBMaxIdleConns:                 11,
		DBConnMaxLifetimeSeconds:       100,
		DBConnMaxIdleTimeSeconds:       50,
		RefreshJobRankIntervalMinutes:  3,
		RefreshRetryDagIntervalSeconds: 9,
	}
	cfg.Init()

	if cfg.DBMaxOpenConns != 77 || cfg.DBMaxIdleConns != 11 ||
		cfg.DBConnMaxLifetimeSeconds != 100 || cfg.DBConnMaxIdleTimeSeconds != 50 ||
		cfg.RefreshJobRankIntervalMinutes != 3 || cfg.RefreshRetryDagIntervalSeconds != 9 {
		t.Fatalf("Init() overwrote explicit values: %+v", cfg)
	}
}

func TestCheckAppliesDefaults(t *testing.T) {
	cfg := Config{
		K8SKubeconfig: map[string]string{"c1": "/path/c1"},
		DBDsn:         map[string]string{"c1": "root:root@tcp(127.0.0.1:3306)/c1"},
	}

	cfg.Check()

	if cfg.DBMaxOpenConns != DefaultDBMaxOpenConns {
		t.Fatalf("Check() did not apply defaults, DBMaxOpenConns = %d", cfg.DBMaxOpenConns)
	}
	if cfg.DBMaxIdleConns != DefaultDBMaxIdleConns {
		t.Fatalf("Check() did not apply defaults, DBMaxIdleConns = %d", cfg.DBMaxIdleConns)
	}
}

func TestCheckPanicsOnInvalidConfig(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Check() did not panic on invalid config")
		}
		if !strings.Contains(fmt.Sprint(r), "k8s config empty") {
			t.Fatalf("Check() panic = %v, want k8s config empty", r)
		}
	}()

	(&Config{}).Check()
}

func TestLoadFromFile(t *testing.T) {
	old := Conf
	t.Cleanup(func() { Conf = old })
	Conf = Config{}

	path := filepath.Join(t.TempDir(), "blitz.yaml")
	content := `
k8s_kubeconfig:
  c1: /path/c1
db_dsn:
  c1: root:root@tcp(127.0.0.1:3306)/blitz
db_max_open_conns: 12
refresh_retry_dag_interval_seconds: 45
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := LoadFromFile(path); err != nil {
		t.Fatalf("LoadFromFile() = %v, want nil", err)
	}

	if got := Conf.K8SKubeconfig["c1"]; got != "/path/c1" {
		t.Fatalf("K8SKubeconfig[c1] = %q, want %q", got, "/path/c1")
	}
	if got := Conf.DBDsn["c1"]; got != "root:root@tcp(127.0.0.1:3306)/blitz" {
		t.Fatalf("DBDsn[c1] = %q, want dsn", got)
	}
	if Conf.DBMaxOpenConns != 12 {
		t.Fatalf("DBMaxOpenConns = %d, want 12", Conf.DBMaxOpenConns)
	}
	if Conf.RefreshRetryDagIntervalSeconds != 45 {
		t.Fatalf("RefreshRetryDagIntervalSeconds = %d, want 45", Conf.RefreshRetryDagIntervalSeconds)
	}
	// LoadFromFile 只负责解析，默认值由调用方通过 Check/Init 填充。
	if Conf.DBMaxIdleConns != 0 {
		t.Fatalf("DBMaxIdleConns = %d, want 0 before Init", Conf.DBMaxIdleConns)
	}
}

func TestLoadFromFileErrors(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		err := LoadFromFile(filepath.Join(t.TempDir(), "not-exist.yaml"))
		if err == nil || !strings.Contains(err.Error(), "read config") {
			t.Fatalf("LoadFromFile() = %v, want read config error", err)
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.yaml")
		if err := os.WriteFile(path, []byte("k8s_kubeconfig: ["), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		err := LoadFromFile(path)
		if err == nil || !strings.Contains(err.Error(), "unmarshal config") {
			t.Fatalf("LoadFromFile() = %v, want unmarshal config error", err)
		}
	})
}
