package web

import (
	"dbcheck/reporter/internal/launcher"
	"fmt"
	"os"
	"path/filepath"
)

type AssetLayoutResolver interface {
	Resolve(executablePath string, cfg launcher.Config) (launcher.AssetLayout, error)
}

type launcherLayoutResolver struct{}

func (launcherLayoutResolver) Resolve(executablePath string, cfg launcher.Config) (launcher.AssetLayout, error) {
	return launcher.ResolveAssetLayout(executablePath, cfg)
}

type ItemInput struct {
	ID      string
	Name    string
	ZipPath string
	// Optional AWR/WDR HTML paths can be added later when HTTP handlers are implemented.
	AWRPath string
	WDRPath string
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

	outDocx := filepath.Join(itemDir, "report.docx")
	cfg := launcher.Config{
		RunDir:  runDir,
		OutDocx: outDocx,
		AWRFile: item.AWRPath,
		WDRFile: item.WDRPath,
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
