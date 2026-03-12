package gaussdb

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"fmt"
	"path/filepath"
	"time"
)

type Collector struct {
	NewRunner  func(cfg cli.Config) (remoteRunner, error)
	CollectSQL func(ctx context.Context, cfg cli.Config, runDir string, writer core.ArtifactWriter) (sqlCollectionResult, error)
	Now        func() time.Time
}

func (c Collector) Collect(ctx context.Context, cfg cli.Config, runDir string, writer core.ArtifactWriter) (map[string]any, error) {
	if writer == nil {
		return nil, core.PrecheckError{Message: "artifact writer is required for gaussdb collector"}
	}
	runnerFactory := c.NewRunner
	if runnerFactory == nil {
		runnerFactory = defaultRunnerFactory
	}
	runner, err := runnerFactory(cfg)
	if err != nil {
		return nil, core.PrecheckError{Message: fmt.Sprintf("gaussdb ssh init failed: %v", err)}
	}
	defer runner.Close()

	metadataOutput, err := runner.Run(buildMetadataCommand(cfg))
	if err != nil {
		return nil, core.PrecheckError{Message: fmt.Sprintf("gaussdb metadata collect failed: %v", err)}
	}
	records := make([]itemRecord, 0, len(itemCatalog))
	errors := make([]string, 0)
	for _, item := range itemCatalog {
		record, collectErr := c.collectItem(runner, cfg, runDir, writer, item)
		if collectErr != nil {
			errors = append(errors, collectErr.Error())
			continue
		}
		records = append(records, record)
	}
	sqlCollector := c.CollectSQL
	if sqlCollector == nil {
		sqlCollector = collectSQLArtifacts
	}
	sqlResult, err := sqlCollector(ctx, cfg, runDir, writer)
	if err != nil {
		return nil, err
	}
	indexPath := filepath.Join(runDir, "gs_check", "index.json")
	if err := writer.WriteJSON(indexPath, map[string]any{"items": toRawIndex(records), "count": len(records)}); err != nil {
		return nil, core.CollectionError{Message: fmt.Sprintf("write gaussdb raw index failed: %v", err)}
	}
	metadata := parseMetadata(metadataOutput, gaussConfig{GaussUser: cfg.GaussUser, GaussEnvFile: cfg.GaussEnvFile})
	return buildPayloadWithSQL(metadata, records, errors, sqlResult), nil
}

func (c Collector) collectItem(
	runner remoteRunner,
	cfg cli.Config,
	runDir string,
	writer core.ArtifactWriter,
	item itemSpec,
) (itemRecord, error) {
	started := c.now()
	command := buildItemCommand(cfg, item.Name)
	output, err := runner.Run(command)
	if err != nil {
		return itemRecord{}, core.CollectionError{Message: fmt.Sprintf("%s failed: %v", item.Name, err)}
	}
	rawFile := filepath.Join(runDir, "gs_check", item.Name+".stdout")
	if err := writer.WriteText(rawFile, output); err != nil {
		return itemRecord{}, core.CollectionError{Message: fmt.Sprintf("write %s raw output failed: %v", item.Name, err)}
	}
	parsed := parseOutput(output)
	details := parseItemDetails(item.Name, parsed)
	return itemRecord{
		Item:             item.Name,
		Domain:           item.Domain,
		Label:            item.Label,
		Status:           parsed.Status,
		NormalizedStatus: normalizeStatus(parsed.Status),
		Summary:          summarizeItem(item.Name, parsed, details),
		Details:          details,
		RawFile:          filepath.ToSlash(filepath.Join("gs_check", item.Name+".stdout")),
		Command:          command,
		DurationMS:       c.now().Sub(started).Milliseconds(),
	}, nil
}

func (c Collector) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}
