package core

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/model"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	collectorLogName = "collector.log"
	manifestName     = "manifest.json"
	resultName       = "result.json"
)

type Runner struct {
	deps Dependencies
}

type runState struct {
	exitCode    int
	moduleStats map[string]model.ModuleStat
	osPayload   map[string]any
	dbPayload   map[string]any
}

func NewRunner(deps Dependencies) (Runner, error) {
	if deps.Clock == nil || deps.DBCollector == nil || deps.OSCollector == nil || deps.Writer == nil {
		return Runner{}, fmt.Errorf("collector dependencies are incomplete")
	}
	if strings.TrimSpace(deps.Version) == "" {
		return Runner{}, fmt.Errorf("collector version is required")
	}
	return Runner{deps: deps}, nil
}

func (r Runner) Run(ctx context.Context, cfg cli.Config) (model.RunArtifacts, error) {
	start := r.deps.Clock.Now()
	runID := buildRunID(cfg.DBType, hostForRunID(cfg), start)
	runDir, err := r.deps.Writer.PrepareRunDir(cfg.OutputDir, runID)
	if err != nil {
		return model.RunArtifacts{}, RunnerError{ExitCode: ExitCollectFailed, Err: err}
	}
	logger := newRunLogger(runID)
	logRunStarted(logger, cfg)
	logger.Info("run_dir_prepared", zap.String("run_dir", runDir))
	state := runState{exitCode: ExitSuccess, moduleStats: map[string]model.ModuleStat{}, osPayload: map[string]any{}, dbPayload: map[string]any{}}
	r.collectOS(ctx, cfg, &state, logger)
	r.collectDB(ctx, cfg, runDir, &state, logger)

	end := r.deps.Clock.Now()
	resultPath, resultObj, err := r.writeResultIfNeeded(runDir, cfg, start, end, &state, logger)
	if err != nil {
		logger.Error("result_write_failed", err)
		_ = r.writeLog(runDir, logger)
		return model.RunArtifacts{}, RunnerError{ExitCode: ExitCollectFailed, Err: err}
	}
	manifest := buildManifest(runID, cfg.DBType, start, end, state, resultPath)
	manifestPath, writeErr := r.writeManifest(runDir, manifest, logger)
	if writeErr != nil {
		logger.Error("manifest_write_failed", writeErr, zap.String("run_dir", runDir))
		_ = r.writeLog(runDir, logger)
		return model.RunArtifacts{}, RunnerError{ExitCode: ExitCollectFailed, Err: writeErr}
	}
	logger.Info("run_finished",
		zap.Int("exit_code", manifest.ExitCode),
		zap.String("overall_status", manifest.OverallStatus),
		zap.Int64("duration_ms", durationMS(start, end)),
	)
	if err := r.writeLog(runDir, logger); err != nil {
		return model.RunArtifacts{}, RunnerError{ExitCode: ExitCollectFailed, Err: err}
	}
	return model.RunArtifacts{
		RunID:        runID,
		RunDir:       runDir,
		ManifestPath: manifestPath,
		ResultPath:   filepath.Join(runDir, resultName),
		LogPath:      filepath.Join(runDir, collectorLogName),
		Manifest:     manifest,
		Result:       resultObj,
	}, nil
}

func logRunStarted(logger *runLogger, cfg cli.Config) {
	logger.Info("run_started",
		zap.String("db_type", cfg.DBType),
		zap.String("db_host", hostForResult(cfg)),
		zap.Int("db_port", cfg.DBPort),
		zap.Bool("os_only", cfg.OSOnly),
		zap.Bool("os_skip", cfg.OSSkip),
		zap.String("output_dir", cfg.OutputDir),
	)
}

func (r Runner) collectOS(ctx context.Context, cfg cli.Config, state *runState, logger *runLogger) {
	logger.Info("os_collect_started")
	if cfg.OSSkip {
		state.moduleStats["os"] = model.ModuleStat{Status: "skipped", DurationMS: 0, Error: nil}
		logger.Info("os_collect_skipped", zap.String("reason", "os_skip"))
		return
	}
	started := r.deps.Clock.Now()
	payload, err := r.deps.OSCollector.Collect(ctx, cfg)
	duration := durationMS(started, r.deps.Clock.Now())
	if err == nil {
		state.moduleStats["os"] = model.ModuleStat{Status: "success", DurationMS: duration, Error: nil}
		state.osPayload = payload
		logger.Info("os_collect_finished", zap.String("status", "success"), zap.Int64("duration_ms", duration), zap.Int("payload_keys", len(payload)))
		return
	}
	msg := err.Error()
	state.moduleStats["os"] = model.ModuleStat{Status: "failed", DurationMS: duration, Error: &msg}
	logger.Error("os_collect_failed", err, zap.Int64("duration_ms", duration))
	if cfg.OSOnly {
		state.exitCode = ExitCollectFailed
		return
	}
	if state.exitCode == ExitSuccess {
		state.exitCode = ExitPartial
	}
}

func (r Runner) collectDB(ctx context.Context, cfg cli.Config, runDir string, state *runState, logger *runLogger) {
	logger.Info("db_collect_started")
	if cfg.OSOnly {
		state.moduleStats["db_basic"] = model.ModuleStat{Status: "skipped", DurationMS: 0, Error: nil}
		logger.Info("db_collect_skipped", zap.String("reason", "os_only"))
		return
	}
	started := r.deps.Clock.Now()
	payload, err := r.deps.DBCollector.Collect(ctx, cfg, runDir, r.deps.Writer)
	duration := durationMS(started, r.deps.Clock.Now())
	if err == nil {
		state.moduleStats["db_basic"] = model.ModuleStat{Status: "success", DurationMS: duration, Error: nil}
		state.dbPayload = payload
		logger.Info("db_collect_finished", zap.String("status", "success"), zap.Int64("duration_ms", duration), zap.Int("payload_keys", len(payload)))
		return
	}
	msg := err.Error()
	state.moduleStats["db_basic"] = model.ModuleStat{Status: "failed", DurationMS: duration, Error: &msg}
	logger.Error("db_collect_failed", err, zap.Int64("duration_ms", duration))
	switch err.(type) {
	case PrecheckError:
		state.exitCode = ExitPrecheckFailed
	default:
		if state.exitCode != ExitPrecheckFailed {
			state.exitCode = ExitCollectFailed
		}
	}
}

func (r Runner) writeResultIfNeeded(runDir string, cfg cli.Config, start time.Time, end time.Time, state *runState, logger *runLogger) (*string, *model.Result, error) {
	if state.exitCode != ExitSuccess && state.exitCode != ExitPartial {
		logger.Info("result_write_skipped", zap.Int("exit_code", state.exitCode))
		return nil, nil, nil
	}
	result := buildResult(r.deps.Version, cfg, start, end, state.osPayload, state.dbPayload)
	resultPath := filepath.Join(runDir, resultName)
	logger.Info("result_write_started", zap.String("result_path", resultPath))
	if err := r.deps.Writer.WriteJSON(resultPath, result); err != nil {
		return nil, nil, err
	}
	logger.Info("result_written", zap.String("result_path", resultPath))
	fileRef := resultName
	return &fileRef, &result, nil
}

func (r Runner) writeManifest(runDir string, manifest model.Manifest, logger *runLogger) (string, error) {
	manifestPath := filepath.Join(runDir, manifestName)
	logger.Info("manifest_write_started", zap.String("manifest_path", manifestPath))
	if err := r.deps.Writer.WriteJSON(manifestPath, manifest); err != nil {
		return "", err
	}
	logger.Info("manifest_written", zap.String("manifest_path", manifestPath))
	return manifestPath, nil
}

func (r Runner) writeLog(runDir string, logger *runLogger) error {
	logger.Sync()
	content := logger.String()
	if strings.TrimSpace(content) == "" {
		content = "1970-01-01 00:00:00.000 WARN collector.runner Collector log is empty run_id=unknown event=log_empty step=0/0\n"
	}
	logPath := filepath.Join(runDir, collectorLogName)
	return r.deps.Writer.WriteText(logPath, content)
}

func buildManifest(runID string, dbType string, start time.Time, end time.Time, state runState, resultPath *string) model.Manifest {
	return model.Manifest{
		SchemaVersion: "1.0",
		RunID:         runID,
		DBType:        dbType,
		StartTime:     start.Format(time.RFC3339),
		EndTime:       end.Format(time.RFC3339),
		ExitCode:      state.exitCode,
		OverallStatus: overallStatus(state.exitCode),
		ModuleStats:   state.moduleStats,
		Artifacts: model.Artifacts{
			Log:     collectorLogName,
			Result:  resultPath,
			Summary: nil,
			Report:  nil,
		},
	}
}

func buildResult(version string, cfg cli.Config, start time.Time, end time.Time, osData map[string]any, dbData map[string]any) model.Result {
	intervalPtr, periodPtr, expected, mode := collectConfig(cfg)
	meta := model.ResultMeta{
		SchemaVersion:    "2.0",
		CollectorVersion: version,
		DBType:           cfg.DBType,
		DBHost:           hostForResult(cfg),
		DBPort:           cfg.DBPort,
		DBName:           cfg.DBName,
		Timezone:         start.Format("Z07:00"),
		CollectTime:      end.Format(time.RFC3339),
	}
	window := model.CollectWindow{
		WindowStart:     start.Format(time.RFC3339),
		WindowEnd:       end.Format(time.RFC3339),
		DurationSeconds: int(end.Sub(start).Seconds()),
	}
	config := model.CollectConfig{SampleMode: mode, SampleIntervalSeconds: intervalPtr, SamplePeriodSeconds: periodPtr, ExpectedSamples: expected}
	return model.Result{Meta: meta, CollectConfig: config, CollectWindow: window, OS: osData, DB: dbData}
}

func collectConfig(cfg cli.Config) (*int, *int, int, string) {
	if cfg.OSCollectInterval <= 0 {
		return nil, nil, 1, "single"
	}
	expected := cfg.OSCollectCount
	if expected <= 0 && cfg.OSCollectDuration > 0 {
		expected = cfg.OSCollectDuration / cfg.OSCollectInterval
		if cfg.OSCollectDuration%cfg.OSCollectInterval != 0 {
			expected++
		}
	}
	if expected <= 0 {
		expected = 1
	}
	interval := cfg.OSCollectInterval
	period := cfg.OSCollectDuration
	if period <= 0 {
		period = expected * cfg.OSCollectInterval
	}
	return &interval, &period, expected, "periodic"
}

func overallStatus(exitCode int) string {
	switch exitCode {
	case ExitSuccess:
		return "success"
	case ExitPartial:
		return "partial_success"
	default:
		return "failed"
	}
}

func buildRunID(dbType string, host string, started time.Time) string {
	return fmt.Sprintf("%s-%s-%s", dbType, sanitizeRunHost(host), started.UTC().Format("20060102T150405Z"))
}

func hostForRunID(cfg cli.Config) string {
	if cfg.DBHost != "" {
		return cfg.DBHost
	}
	if cfg.Local {
		return "localhost"
	}
	return "unknown"
}

func hostForResult(cfg cli.Config) string {
	if cfg.DBHost != "" {
		return cfg.DBHost
	}
	return "localhost"
}

func sanitizeRunHost(host string) string {
	host = strings.TrimSpace(host)
	host = strings.ReplaceAll(host, ":", "_")
	host = strings.ReplaceAll(host, "/", "_")
	if host == "" {
		return "unknown"
	}
	return host
}

func durationMS(start time.Time, end time.Time) int64 {
	if end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}
