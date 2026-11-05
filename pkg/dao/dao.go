package dao

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/binbinpeng123/blitz/pkg/conf"
	jmoironsqlx "github.com/jmoiron/sqlx"
	slardarsqlx "gitlab.hobot.cc/aidi/slardar/sqlx"
)

type dbPoolConfig struct {
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

func (cfg dbPoolConfig) validate() error {
	if cfg.maxOpenConns <= 0 {
		return fmt.Errorf("db_max_open_conns must be positive, got %d", cfg.maxOpenConns)
	}
	if cfg.maxIdleConns < 0 {
		return fmt.Errorf("db_max_idle_conns must not be negative, got %d", cfg.maxIdleConns)
	}
	if cfg.maxIdleConns > cfg.maxOpenConns {
		return fmt.Errorf("db_max_idle_conns %d exceeds db_max_open_conns %d", cfg.maxIdleConns, cfg.maxOpenConns)
	}
	if cfg.connMaxLifetime <= 0 {
		return fmt.Errorf("db_conn_max_lifetime must be positive, got %s", cfg.connMaxLifetime)
	}
	if cfg.connMaxIdleTime <= 0 {
		return fmt.Errorf("db_conn_max_idle_time must be positive, got %s", cfg.connMaxIdleTime)
	}
	return nil
}

type poolOpener func(ctx context.Context, cluster, dsn string, cfg dbPoolConfig) (*slardarsqlx.DB, error)

func openDBPool(ctx context.Context, cluster, dsn string, cfg dbPoolConfig) (*slardarsqlx.DB, error) {
	db, err := jmoironsqlx.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql driver: %w", err)
	}
	db.SetMaxOpenConns(cfg.maxOpenConns)
	db.SetMaxIdleConns(cfg.maxIdleConns)
	db.SetConnMaxLifetime(cfg.connMaxLifetime)
	db.SetConnMaxIdleTime(cfg.connMaxIdleTime)
	db = db.Unsafe()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database for cluster %s: %w", cluster, err)
	}

	return &slardarsqlx.DB{
		DB: db,
	}, nil
}

var defaultDBRegistry = newDBRegistry(openDBPool)

type Dao struct {
	DB *slardarsqlx.DB
}

type dbRegistry struct {
	mu          sync.RWMutex
	connections map[string]*Dao
	initialized bool
	openPool    poolOpener
}

func newDBRegistry(openPool poolOpener) *dbRegistry {
	return &dbRegistry{
		connections: make(map[string]*Dao),
		openPool:    openPool,
	}
}

func (r *dbRegistry) init(ctx context.Context, clusters []string, dsns map[string]string, poolCfg dbPoolConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.initialized {
		return nil
	}
	if len(clusters) == 0 {
		return errors.New("k8s config empty")
	}
	if err := poolCfg.validate(); err != nil {
		return err
	}

	orderedClusters := append([]string(nil), clusters...)
	sort.Strings(orderedClusters)
	for _, cluster := range orderedClusters {
		if strings.TrimSpace(dsns[cluster]) == "" {
			return fmt.Errorf("db_dsn missing for cluster %s", cluster)
		}
	}

	connections := make(map[string]*Dao, len(orderedClusters))
	for _, cluster := range orderedClusters {
		db, err := r.openPool(ctx, cluster, dsns[cluster], poolCfg)
		if err != nil {
			openErr := fmt.Errorf("open database pool for cluster %s: %w", cluster, err)
			if closeErr := closeConnections(connections); closeErr != nil {
				return errors.Join(openErr, fmt.Errorf("close partially initialized database pools: %w", closeErr))
			}
			return openErr
		}
		connections[cluster] = &Dao{DB: db}
	}

	r.initialized = true
	r.connections = connections
	return nil
}

func (r *dbRegistry) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := closeConnections(r.connections)
	r.connections = make(map[string]*Dao)
	r.initialized = false
	return err
}

func (r *dbRegistry) get(k8s string) *Dao {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.connections[k8s]
}

func closeConnections(connections map[string]*Dao) error {
	clusters := make([]string, 0, len(connections))
	for cluster := range connections {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)

	var errs []error
	for _, cluster := range clusters {
		db := connections[cluster]
		if db == nil || db.DB == nil {
			continue
		}
		if err := db.DB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close database pool for cluster %s: %w", cluster, err))
		}
	}

	return errors.Join(errs...)
}

func InitDB(ctx context.Context) error {
	if err := conf.Conf.ValidateDatabase(); err != nil {
		return err
	}
	clusters := make([]string, 0, len(conf.Conf.K8SKubeconfig))
	for cluster := range conf.Conf.K8SKubeconfig {
		clusters = append(clusters, cluster)
	}
	return defaultDBRegistry.init(ctx, clusters, conf.Conf.DBDsn, dbPoolConfig{
		maxOpenConns:    conf.Conf.DBMaxOpenConns,
		maxIdleConns:    conf.Conf.DBMaxIdleConns,
		connMaxLifetime: time.Duration(conf.Conf.DBConnMaxLifetimeSeconds) * time.Second,
		connMaxIdleTime: time.Duration(conf.Conf.DBConnMaxIdleTimeSeconds) * time.Second,
	})
}

func Get(k8s string) *Dao {
	return defaultDBRegistry.get(k8s)
}

func CloseDB() error {
	return defaultDBRegistry.close()
}
