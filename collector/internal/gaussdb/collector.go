package gaussdb

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"fmt"
	"time"
)

type Collector struct {
	CollectSQL func(ctx context.Context, cfg cli.Config, runDir string, writer core.ArtifactWriter) (sqlCollectionResult, error)
	Now        func() time.Time
	Progress   core.ProgressReporter
	RunID      string
}

func (c Collector) WithProgress(runID string, progress core.ProgressReporter) core.DBCollector {
	c.RunID = runID
	c.Progress = progress
	return c
}

func (c Collector) Collect(ctx context.Context, cfg cli.Config, runDir string, writer core.ArtifactWriter) (map[string]any, error) {
	if writer == nil {
		return nil, core.PrecheckError{Message: "artifact writer is required for gaussdb collector"}
	}
	sqlCollector := c.CollectSQL
	if sqlCollector == nil {
		sqlCollector = c.collectSQLFirstArtifacts
	}
	sqlResult, err := sqlCollector(ctx, cfg, runDir, writer)
	if err != nil {
		return nil, err
	}
	return buildPayloadWithSQL(sqlResult.Metadata, sqlResult.Records, nil, sqlResult), nil
}

func (c Collector) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func ignoredGaussShellOptions(cfg cli.Config) []string {
	ignored := make([]string, 0, 2)
	if cfg.GaussUser != "" {
		ignored = append(ignored, "--gauss-user")
	}
	if cfg.GaussEnvFile != "" {
		ignored = append(ignored, "--gauss-env-file")
	}
	return ignored
}

func collectionError(scope string, err error) error {
	return core.CollectionError{Message: fmt.Sprintf("%s failed: %v", scope, err)}
}
