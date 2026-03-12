package core

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/model"
)

type DBCollector interface {
	Collect(ctx context.Context, cfg cli.Config, runDir string, writer ArtifactWriter) (map[string]any, error)
}

type OSCollector interface {
	Collect(ctx context.Context, cfg cli.Config) (map[string]any, error)
}

type ArtifactWriter interface {
	PrepareRunDir(outputDir string, runID string) (string, error)
	WriteJSON(path string, v any) error
	WriteText(path string, content string) error
}

type Dependencies struct {
	Clock       model.Clock
	DBCollector DBCollector
	OSCollector OSCollector
	Writer      ArtifactWriter
	Version     string
}
