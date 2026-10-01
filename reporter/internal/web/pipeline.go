package web

import (
	"context"
	"dbcheck/reporter/internal/launcher"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ReportPipeline generates one report item's document. Production uses
// *Pipeline; the contract test server plugs in a stub.
type ReportPipeline interface {
	// RunItem generates the item's report at ItemReportPath(job.TaskDir,
	// job.Input.ID), passing its output lines to onLog.
	RunItem(ctx context.Context, job ItemJob, onLog func(LogEvent)) ItemResult
}

// ItemJob is one report item handed to the pipeline.
type ItemJob struct {
	TaskID        string
	TaskDir       string
	TaskCreatedAt time.Time
	Input         ItemInput
}

// ItemReportPath is where an item's report document is written.
func ItemReportPath(taskDir, itemID string) string {
	return filepath.Join(taskDir, "items", itemID, "report.docx")
}

type AssetLayoutResolver interface {
	Resolve(executablePath string, cfg launcher.Config) (launcher.AssetLayout, error)
}

type launcherLayoutResolver struct{}

func (launcherLayoutResolver) Resolve(executablePath string, cfg launcher.Config) (launcher.AssetLayout, error) {
	return launcher.ResolveAssetLayout(executablePath, cfg)
}

type ItemInput struct {
	ID       string
	Name     string
	ZipPath  string
	AWRPath  string
	WDRPaths []string
}

type ItemStatus string

const (
	ItemDone   ItemStatus = "done"
	ItemFailed ItemStatus = "failed"
)

type ItemResult struct {
	ID         string
	Status     ItemStatus
	ReportDocx string
	Error      string
}

type Pipeline struct {
	ExecutablePath string
	PythonBin      string

	LayoutResolver AssetLayoutResolver
	Runner         CommandRunner

	ExtractZip func(zipPath string, destDir string) error
	DetectRun  func(root string) (string, error)
}

func NewPipeline(executablePath string, pythonBin string) *Pipeline {
	return &Pipeline{
		ExecutablePath: executablePath,
		PythonBin:      pythonBin,
		LayoutResolver: launcherLayoutResolver{},
		Runner:         NewExecRunner(),
		ExtractZip:     ExtractZipFile,
		DetectRun:      DetectRunDirByManifest,
	}
}

func (p *Pipeline) RunItems(taskDir string, items []ItemInput, onLog func(itemID string, ev LogEvent)) []ItemResult {
	results := make([]ItemResult, 0, len(items))
	for _, item := range items {
		result := p.runOne(taskDir, item, onLog)
		results = append(results, result)
	}
	return results
}

// RunItem runs the report launcher on one item.
func (p *Pipeline) RunItem(_ context.Context, job ItemJob, onLog func(LogEvent)) ItemResult {
	return p.runOne(job.TaskDir, job.Input, func(_ string, ev LogEvent) { onLog(ev) })
}

func (p *Pipeline) runOne(taskDir string, item ItemInput, onLog func(itemID string, ev LogEvent)) ItemResult {
	if err := validateTaskID(item.ID); err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: err.Error()}
	}

	itemDir := filepath.Join(taskDir, "items", item.ID)
	extractDir := filepath.Join(itemDir, "extract")
	if err := os.RemoveAll(extractDir); err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: fmt.Errorf("cleanup extract dir failed: %w", err).Error()}
	}
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: fmt.Errorf("create extract dir failed: %w", err).Error()}
	}

	if err := p.ExtractZip(item.ZipPath, extractDir); err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: fmt.Errorf("extract zip failed: %w", err).Error()}
	}
	runDir, err := p.DetectRun(extractDir)
	if err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: fmt.Errorf("detect run dir failed: %w", err).Error()}
	}
	dbType, err := launcher.DetectRunDirDBType(runDir)
	if err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: fmt.Errorf("detect db type failed: %w", err).Error()}
	}
	if err := validateHTMLAttachment(dbType, item); err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: err.Error()}
	}

	outDocx := ItemReportPath(taskDir, item.ID)
	cfg := launcher.Config{
		RunDir:   runDir,
		OutDocx:  outDocx,
		AWRFile:  item.AWRPath,
		WDRFiles: item.WDRPaths,
	}
	layout, err := p.LayoutResolver.Resolve(p.ExecutablePath, cfg)
	if err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: fmt.Errorf("resolve asset layout failed: %w", err).Error()}
	}
	args := launcher.OrchestratorArgs(cfg, layout)

	if err := p.Runner.Run(p.PythonBin, append([]string{layout.Script}, args...), func(ev LogEvent) {
		if onLog != nil {
			onLog(item.ID, ev)
		}
	}); err != nil {
		return ItemResult{ID: item.ID, Status: ItemFailed, Error: fmt.Errorf("orchestrator failed: %w", err).Error()}
	}

	return ItemResult{ID: item.ID, Status: ItemDone, ReportDocx: outDocx}
}

func validateHTMLAttachment(dbType string, item ItemInput) error {
	hasAWR := item.AWRPath != ""
	hasWDR := len(item.WDRPaths) != 0
	switch dbType {
	case "oracle":
		if hasWDR {
			return fmt.Errorf("wdr file is only supported for GaussDB run-dir, current db_type=%q", dbType)
		}
	case "gaussdb":
		if hasAWR {
			return fmt.Errorf("awr file is only supported for Oracle run-dir, current db_type=%q", dbType)
		}
	case "mysql":
		if hasAWR || hasWDR {
			return fmt.Errorf("AWR/WDR HTML is not supported for MySQL run-dir")
		}
	default:
		if hasAWR || hasWDR {
			return fmt.Errorf("AWR/WDR HTML requires mysql, oracle, or gaussdb run-dir, current db_type=%q", dbType)
		}
	}
	return nil
}
