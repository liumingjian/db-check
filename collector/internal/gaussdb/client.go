package gaussdb

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"fmt"
	"strings"
	"time"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
)

func openDB(ctx context.Context, cfg cli.Config) (*sql.DB, error) {
	dsn := buildDSN(cfg)
	db, err := sql.Open("opengauss", dsn)
	if err != nil {
		return nil, core.PrecheckError{Message: fmt.Sprintf("open gaussdb connection failed: %v", err)}
	}
	timeout := time.Duration(cfg.SQLTimeoutSeconds) * time.Second
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, core.PrecheckError{Message: fmt.Sprintf("gaussdb ping failed: %v", err)}
	}
	return db, nil
}

func buildDSN(cfg cli.Config) string {
	parts := []string{
		"host=" + strings.TrimSpace(cfg.DBHost),
		fmt.Sprintf("port=%d", cfg.DBPort),
		"user=" + strings.TrimSpace(cfg.DBUsername),
		"password=" + cfg.DBPassword,
		"dbname=" + strings.TrimSpace(cfg.DBName),
		fmt.Sprintf("connect_timeout=%d", cfg.SQLTimeoutSeconds),
		"sslmode=disable",
		"application_name=dbcheck",
	}
	return strings.Join(parts, " ")
}
