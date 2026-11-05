package dao

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/blitz/pkg/conf"
	jmoironsqlx "github.com/jmoiron/sqlx"
	slardarsqlx "gitlab.hobot.cc/aidi/slardar/sqlx"
)

// fakeConnector 提供一个可以安全 Close 的 *sql.DB，避免测试依赖真实 MySQL。
type fakeConnector struct{}

func (fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn{}, nil }
func (fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return fakeConn{}, nil }

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }

func newFakeDB(t *testing.T) *slardarsqlx.DB {
	t.Helper()
	sdb := sql.OpenDB(fakeConnector{})
	t.Cleanup(func() { _ = sdb.Close() })
	return &slardarsqlx.DB{DB: jmoironsqlx.NewDb(sdb, "fake")}
}

func validPoolConfig() dbPoolConfig {
	return dbPoolConfig{
		maxOpenConns:    30,
		maxIdleConns:    10,
		connMaxLifetime: time.Hour,
		connMaxIdleTime: time.Minute,
	}
}

type openCall struct {
	cluster string
	dsn     string
	cfg     dbPoolConfig
}

func newRecordingRegistry(t *testing.T, calls *[]openCall) *dbRegistry {
	t.Helper()
	return newDBRegistry(func(_ context.Context, cluster, dsn string, cfg dbPoolConfig) (*slardarsqlx.DB, error) {
		*calls = append(*calls, openCall{cluster: cluster, dsn: dsn, cfg: cfg})
		return newFakeDB(t), nil
	})
}

func TestDBPoolConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*dbPoolConfig)
		wantErr string
	}{
		{name: "valid", mutate: func(*dbPoolConfig) {}},
		{
			name:    "max open conns not positive",
			mutate:  func(c *dbPoolConfig) { c.maxOpenConns = 0 },
			wantErr: "db_max_open_conns must be positive",
		},
		{
			name:    "max idle conns negative",
			mutate:  func(c *dbPoolConfig) { c.maxIdleConns = -1 },
			wantErr: "db_max_idle_conns must not be negative",
		},
		{
			name:    "max idle conns exceeds max open conns",
			mutate:  func(c *dbPoolConfig) { c.maxIdleConns = c.maxOpenConns + 1 },
			wantErr: "exceeds db_max_open_conns",
		},
		{
			name:    "conn max lifetime not positive",
			mutate:  func(c *dbPoolConfig) { c.connMaxLifetime = 0 },
			wantErr: "db_conn_max_lifetime must be positive",
		},
		{
			name:    "conn max idle time not positive",
			mutate:  func(c *dbPoolConfig) { c.connMaxIdleTime = 0 },
			wantErr: "db_conn_max_idle_time must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validPoolConfig()
			tt.mutate(&cfg)

			err := cfg.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRegistryInit(t *testing.T) {
	var calls []openCall
	reg := newRecordingRegistry(t, &calls)

	dsns := map[string]string{"a": "dsn-a", "b": "dsn-b"}
	cfg := validPoolConfig()
	if err := reg.init(context.Background(), []string{"b", "a"}, dsns, cfg); err != nil {
		t.Fatalf("init() = %v, want nil", err)
	}

	// 集群按名字排序后依次初始化，保证启动过程可复现。
	if got, want := []string{calls[0].cluster, calls[1].cluster}, []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("open order = %v, want %v", got, want)
	}
	for _, c := range calls {
		if c.dsn != dsns[c.cluster] {
			t.Fatalf("cluster %s dsn = %q, want %q", c.cluster, c.dsn, dsns[c.cluster])
		}
		if c.cfg != cfg {
			t.Fatalf("cluster %s pool config = %+v, want %+v", c.cluster, c.cfg, cfg)
		}
	}

	if reg.get("a") == nil || reg.get("b") == nil {
		t.Fatal("get() returned nil for initialized cluster")
	}
	if reg.get("unknown") != nil {
		t.Fatal("get() should return nil for unknown cluster")
	}

	// 重复 init 应当幂等，不会重复创建连接池。
	if err := reg.init(context.Background(), []string{"a", "b"}, dsns, cfg); err != nil {
		t.Fatalf("second init() = %v, want nil", err)
	}
	if len(calls) != 2 {
		t.Fatalf("open called %d times, want 2", len(calls))
	}
}

func TestRegistryInitRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		clusters []string
		dsns     map[string]string
		mutate   func(*dbPoolConfig)
		wantErr  string
	}{
		{
			name:     "no clusters",
			clusters: nil,
			dsns:     map[string]string{},
			wantErr:  "k8s config empty",
		},
		{
			name:     "missing dsn",
			clusters: []string{"a"},
			dsns:     map[string]string{"a": " "},
			wantErr:  "db_dsn missing for cluster a",
		},
		{
			name:     "invalid pool config",
			clusters: []string{"a"},
			dsns:     map[string]string{"a": "dsn-a"},
			mutate:   func(c *dbPoolConfig) { c.maxOpenConns = 0 },
			wantErr:  "db_max_open_conns must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls []openCall
			reg := newRecordingRegistry(t, &calls)

			cfg := validPoolConfig()
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			err := reg.init(context.Background(), tt.clusters, tt.dsns, cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("init() = %v, want error containing %q", err, tt.wantErr)
			}
			if len(calls) != 0 {
				t.Fatalf("open called %d times, want 0", len(calls))
			}
		})
	}
}

func TestRegistryInitClosesPoolsOnFailure(t *testing.T) {
	opened := make(map[string]*slardarsqlx.DB)
	reg := newDBRegistry(func(_ context.Context, cluster, _ string, _ dbPoolConfig) (*slardarsqlx.DB, error) {
		if cluster == "b" {
			return nil, errors.New("boom")
		}
		db := newFakeDB(t)
		opened[cluster] = db
		return db, nil
	})

	err := reg.init(context.Background(), []string{"a", "b"}, map[string]string{"a": "dsn-a", "b": "dsn-b"}, validPoolConfig())
	if err == nil || !strings.Contains(err.Error(), "open database pool for cluster b") {
		t.Fatalf("init() = %v, want error for cluster b", err)
	}
	if reg.get("a") != nil {
		t.Fatal("registry should stay empty after failed init")
	}
	if got := opened["a"]; got == nil {
		t.Fatal("cluster a pool was not opened")
	} else if err := got.Ping(); err == nil {
		t.Fatal("pool opened before failure should be closed")
	}
}

func TestRegistryCloseAndReinit(t *testing.T) {
	var calls []openCall
	reg := newRecordingRegistry(t, &calls)
	dsns := map[string]string{"a": "dsn-a"}

	if err := reg.init(context.Background(), []string{"a"}, dsns, validPoolConfig()); err != nil {
		t.Fatalf("init() = %v, want nil", err)
	}
	if err := reg.close(); err != nil {
		t.Fatalf("close() = %v, want nil", err)
	}
	if reg.get("a") != nil {
		t.Fatal("get() should return nil after close")
	}

	if err := reg.init(context.Background(), []string{"a"}, dsns, validPoolConfig()); err != nil {
		t.Fatalf("re-init() = %v, want nil", err)
	}
	if len(calls) != 2 {
		t.Fatalf("open called %d times, want 2", len(calls))
	}
}

// TestInitDBConvertsSecondsToDuration 覆盖 time.Duration(秒) 被当成纳秒的回归问题。
func TestInitDBConvertsSecondsToDuration(t *testing.T) {
	oldConf := conf.Conf
	oldRegistry := defaultDBRegistry
	t.Cleanup(func() {
		conf.Conf = oldConf
		defaultDBRegistry = oldRegistry
	})

	var gotCfg dbPoolConfig
	var gotClusters []string
	defaultDBRegistry = newDBRegistry(func(_ context.Context, cluster, _ string, cfg dbPoolConfig) (*slardarsqlx.DB, error) {
		gotClusters = append(gotClusters, cluster)
		gotCfg = cfg
		return newFakeDB(t), nil
	})
	conf.Conf = conf.Config{
		K8SKubeconfig:                  map[string]string{"c1": "/path/c1", "c2": "/path/c2"},
		DBDsn:                          map[string]string{"c1": "dsn-c1", "c2": "dsn-c2"},
		DBMaxOpenConns:                 30,
		DBMaxIdleConns:                 10,
		DBConnMaxLifetimeSeconds:       40,
		DBConnMaxIdleTimeSeconds:       10,
		RefreshJobRankIntervalMinutes:  5,
		RefreshRetryDagIntervalSeconds: 60,
	}

	if err := InitDB(context.Background()); err != nil {
		t.Fatalf("InitDB() = %v, want nil", err)
	}

	if gotCfg.connMaxLifetime != 40*time.Second {
		t.Fatalf("connMaxLifetime = %s, want %s", gotCfg.connMaxLifetime, 40*time.Second)
	}
	if gotCfg.connMaxIdleTime != 10*time.Second {
		t.Fatalf("connMaxIdleTime = %s, want %s", gotCfg.connMaxIdleTime, 10*time.Second)
	}
	sort.Strings(gotClusters)
	if want := []string{"c1", "c2"}; !reflect.DeepEqual(gotClusters, want) {
		t.Fatalf("opened clusters = %v, want %v", gotClusters, want)
	}
	if Get("c1") == nil {
		t.Fatal("Get(c1) = nil after InitDB")
	}
}

func TestInitDBValidatesConfig(t *testing.T) {
	oldConf := conf.Conf
	oldRegistry := defaultDBRegistry
	t.Cleanup(func() {
		conf.Conf = oldConf
		defaultDBRegistry = oldRegistry
	})

	calls := 0
	defaultDBRegistry = newDBRegistry(func(_ context.Context, _, _ string, _ dbPoolConfig) (*slardarsqlx.DB, error) {
		calls++
		return newFakeDB(t), nil
	})
	conf.Conf = conf.Config{}

	err := InitDB(context.Background())
	if err == nil || !strings.Contains(err.Error(), "k8s config empty") {
		t.Fatalf("InitDB() = %v, want k8s config empty error", err)
	}
	if calls != 0 {
		t.Fatalf("open called %d times, want 0", calls)
	}
}

func TestOpenDBPoolOpenFailure(t *testing.T) {
	_, err := openDBPool(context.Background(), "c1", "not-a-valid-dsn", validPoolConfig())
	if err == nil || !strings.Contains(err.Error(), "open mysql driver") {
		t.Fatalf("openDBPool() = %v, want open driver error", err)
	}
}

func TestOpenDBPoolPingFailure(t *testing.T) {
	// 端口 1 上没有服务，连接会立即被拒绝，用于覆盖 ping 失败分支。
	_, err := openDBPool(context.Background(), "c1", "root:root@tcp(127.0.0.1:1)/blitz", validPoolConfig())
	if err == nil || !strings.Contains(err.Error(), "ping database for cluster c1") {
		t.Fatalf("openDBPool() = %v, want ping error", err)
	}
}
