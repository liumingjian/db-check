package oracle

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
)

type Collector struct{}

type metricsCollector struct {
	db     *sql.DB
	cfg    cli.Config
	errors []string
}

func (Collector) Collect(ctx context.Context, cfg cli.Config, _ string, _ core.ArtifactWriter) (map[string]any, error) {
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

func newMetricsCollector(db *sql.DB, cfg cli.Config) *metricsCollector {
	return &metricsCollector{db: db, cfg: cfg, errors: []string{}}
}

func (c *metricsCollector) collectAll(ctx context.Context) map[string]any {
	return map[string]any{
		"basic_info":   c.collectBasicInfo(ctx),
		"config_check": c.collectConfigCheck(ctx),
		"storage":      c.collectStorage(ctx),
		"backup":       c.collectBackup(ctx),
		"performance":  c.collectPerformance(ctx),
		"sql_analysis": c.collectSQLAnalysis(ctx),
		"security":     c.collectSecurity(ctx),
	}
}
