package oracle

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"fmt"
	"strings"
	"time"

	_ "github.com/sijms/go-ora/v2"
	go_ora "github.com/sijms/go-ora/v2"
)

func openDB(ctx context.Context, cfg cli.Config) (*sql.DB, error) {
	dsn := buildDSN(cfg)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, core.PrecheckError{Message: fmt.Sprintf("open oracle connection failed: %v", err)}
	}
	timeout := time.Duration(cfg.SQLTimeoutSeconds) * time.Second
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, core.PrecheckError{Message: fmt.Sprintf("oracle ping failed: %v", err)}
	}
	return db, nil
}

func buildDSN(cfg cli.Config) string {
	options := map[string]string{
		"SID": strings.TrimSpace(cfg.DBName),
	}
	return go_ora.BuildUrl(
		strings.TrimSpace(cfg.DBHost),
		cfg.DBPort,
		"",
		cfg.DBUsername,
		cfg.DBPassword,
		options,
	)
}
