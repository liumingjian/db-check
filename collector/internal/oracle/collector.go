package oracle

import (
	"context"
	"database/sql"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
)

type Collector struct{}

type metricsCollector struct {
	db                *sql.DB
	cfg               cli.Config
	errors            []string
	availability      map[string]any
	inspectionAccount map[string]any
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
	return &metricsCollector{db: db, cfg: cfg, errors: []string{}, availability: map[string]any{}}
}

func (c *metricsCollector) collectAll(ctx context.Context) map[string]any {
	version, topology := c.collectDeployment(ctx)
	payload := map[string]any{
		"version_info":        version,
		"deployment_topology": topology,
		"basic_info":          c.collectBasicInfo(ctx),
		"config_check":        c.collectConfigCheck(ctx),
		"storage":             c.collectStorage(ctx),
		"alert_log":           c.collectAlertLog(ctx, version),
		"backup":              c.collectBackup(ctx),
		"performance":         c.collectPerformance(ctx),
		"sql_analysis":        c.collectSQLAnalysis(ctx),
		"security":            c.collectSecurity(ctx),
		"data_guard":          c.collectDataGuard(ctx, topology),
		"asm":                 c.collectASM(ctx, topology),
		"rac":                 c.collectRAC(ctx, topology),
		"host_checks":         c.collectHostChecks(ctx, topology),
	}
	payload["inspection_account"] = c.inspectionAccount
	payload["collection_availability"] = c.availability
	return payload
}
