package mysql

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Collector struct{}

func (Collector) Collect(ctx context.Context, cfg cli.Config) (map[string]any, error) {
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	collector := newMetricsCollector(db, cfg)
	payload := collector.collectAll(ctx)
	if len(collector.errors) > 0 {
		payload["collect_errors"] = collector.errors
	}
	return payload, nil
}

func openDB(ctx context.Context, cfg cli.Config) (*sql.DB, error) {
	dsn := buildDSN(cfg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, core.PrecheckError{Message: fmt.Sprintf("open mysql connection failed: %v", err)}
	}
	timeout := time.Duration(cfg.SQLTimeoutSeconds) * time.Second
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, core.PrecheckError{Message: fmt.Sprintf("mysql ping failed: %v", err)}
	}
	return db, nil
}

func buildDSN(cfg cli.Config) string {
	host := strings.TrimSpace(cfg.DBHost)
	if host == "" {
		host = "127.0.0.1"
	}
	timeout := cfg.SQLTimeoutSeconds
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
		cfg.DBUsername,
		cfg.DBPassword,
		host,
		cfg.DBPort,
		cfg.DBName,
		timeout,
		timeout,
		timeout,
	)
}
