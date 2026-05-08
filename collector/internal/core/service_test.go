package core

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/model"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

type stubDBCollector struct {
	payload map[string]any
	err     error
}

func (s stubDBCollector) Collect(_ context.Context, _ cli.Config, _ string, _ ArtifactWriter) (map[string]any, error) {
	return s.payload, s.err
}

type stubOSCollector struct {
	payload map[string]any
	err     error
	called  *bool
}

func (s stubOSCollector) Collect(_ context.Context, _ cli.Config) (map[string]any, error) {
	if s.called != nil {
		*s.called = true
	}
	return s.payload, s.err
}

type memoryWriter struct {
	files map[string][]byte
}

func newMemoryWriter() *memoryWriter {
	return &memoryWriter{files: map[string][]byte{}}
}

func (w *memoryWriter) PrepareRunDir(outputDir string, runID string) (string, error) {
	return fmt.Sprintf("%s/%s", outputDir, runID), nil
}

func (w *memoryWriter) WriteJSON(path string, v any) error {
	bytes, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.files[path] = bytes
	return nil
}

func (w *memoryWriter) WriteText(path string, content string) error {
	w.files[path] = []byte(content)
	return nil
}

func TestRunnerSuccessWithOSOnly(t *testing.T) {
	writer := newMemoryWriter()
	runner, err := NewRunner(Dependencies{
		Clock:       fixedClock{now: time.Date(2026, 3, 5, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))},
		DBCollector: stubDBCollector{},
		OSCollector: stubOSCollector{payload: map[string]any{"system_info": map[string]any{"hostname": "demo"}}},
		Writer:      writer,
		Version:     "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	cfg := cli.Config{DBType: "mysql", OSOnly: true, OutputDir: "./runs", DBPort: 3306}
	artifacts, err := runner.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if artifacts.Manifest.ExitCode != ExitSuccess {
		t.Fatalf("expected exit 0, got %d", artifacts.Manifest.ExitCode)
	}
	if artifacts.Result == nil {
		t.Fatalf("expected result to be written")
	}
}

func TestRunnerPrecheckFailureReturnsExit30(t *testing.T) {
	writer := newMemoryWriter()
	runner, err := NewRunner(Dependencies{
		Clock:       fixedClock{now: time.Date(2026, 3, 5, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))},
		DBCollector: stubDBCollector{err: PrecheckError{Message: "ping failed"}},
		OSCollector: stubOSCollector{payload: map[string]any{"system_info": map[string]any{"hostname": "demo"}}},
		Writer:      writer,
		Version:     "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	cfg := cli.Config{
		DBType: "mysql", OutputDir: "./runs", DBHost: "127.0.0.1", DBPort: 3306,
		DBUsername: "u", DBPassword: "p", DBName: "d",
	}
	artifacts, err := runner.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if artifacts.Manifest.ExitCode != ExitPrecheckFailed {
		t.Fatalf("expected exit 30, got %d", artifacts.Manifest.ExitCode)
	}
	if artifacts.Result != nil {
		t.Fatalf("expected no result on precheck failure")
	}
}

func TestRunnerNoOSInfoProducesPartialManifestAndResult(t *testing.T) {
	writer := newMemoryWriter()
	osCalled := false
	runner, err := NewRunner(Dependencies{
		Clock:       fixedClock{now: time.Date(2026, 3, 5, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))},
		DBCollector: stubDBCollector{payload: map[string]any{"basic_info": map[string]any{"version": "19c"}}},
		OSCollector: stubOSCollector{called: &osCalled},
		Writer:      writer,
		Version:     "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	cfg := cli.Config{DBType: "oracle", OutputDir: "./runs", DBHost: "10.0.0.8", DBPort: 1521}
	artifacts, err := runner.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if osCalled {
		t.Fatalf("expected OS collector not to be called")
	}
	if artifacts.Manifest.ExitCode != ExitPartial {
		t.Fatalf("expected exit 10, got %d", artifacts.Manifest.ExitCode)
	}
	if artifacts.Manifest.OverallStatus != "partial_success" {
		t.Fatalf("expected partial_success, got %s", artifacts.Manifest.OverallStatus)
	}
	if artifacts.Manifest.ModuleStats["os"].Status != "skipped" {
		t.Fatalf("expected os skipped, got %+v", artifacts.Manifest.ModuleStats["os"])
	}
	if artifacts.Result == nil {
		t.Fatalf("expected result to be written")
	}
	if _, ok := writer.files[artifacts.ResultPath]; !ok {
		t.Fatalf("result.json not written: %s", artifacts.ResultPath)
	}
}

func TestRunnerWritesStructuredCollectorLog(t *testing.T) {
	writer := newMemoryWriter()
	runner, err := NewRunner(Dependencies{
		Clock:       fixedClock{now: time.Date(2026, 3, 9, 10, 0, 0, 0, time.FixedZone("CST", 8*3600))},
		DBCollector: stubDBCollector{payload: map[string]any{"basic_info": map[string]any{"is_alive": true}}},
		OSCollector: stubOSCollector{payload: map[string]any{"system_info": map[string]any{"hostname": "demo"}}},
		Writer:      writer,
		Version:     "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	cfg := cli.Config{DBType: "mysql", OutputDir: "./runs", DBHost: "127.0.0.1", DBPort: 3306}
	artifacts, runErr := runner.Run(context.Background(), cfg)
	if runErr != nil {
		t.Fatalf("Run failed: %v", runErr)
	}
	raw, ok := writer.files[artifacts.LogPath]
	if !ok {
		t.Fatalf("collector.log not written: %s", artifacts.LogPath)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) == 0 {
		t.Fatalf("collector.log is empty")
	}
	first := lines[0]
	if strings.HasPrefix(strings.TrimSpace(first), "{") {
		t.Fatalf("expected text-style log line, got json: %s", first)
	}
	if !strings.Contains(first, "Run started") {
		t.Fatalf("expected first line to contain human message, got: %s", first)
	}
	if !strings.Contains(first, "run_started") {
		t.Fatalf("expected first line to contain event token run_started, got: %s", first)
	}
}

func TestRunnerEmitsProgressEvents(t *testing.T) {
	writer := newMemoryWriter()
	events := []ProgressEvent{}
	runner, err := NewRunner(Dependencies{
		Clock:       fixedClock{now: time.Date(2026, 3, 9, 10, 0, 0, 0, time.FixedZone("CST", 8*3600))},
		DBCollector: stubDBCollector{payload: map[string]any{"basic_info": map[string]any{"is_alive": true}}},
		OSCollector: stubOSCollector{},
		Writer:      writer,
		Version:     "1.0.0",
		Progress: func(event ProgressEvent) {
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}
	cfg := cli.Config{DBType: "mysql", OutputDir: "./runs", DBHost: "127.0.0.1", DBPort: 3306}
	if _, err := runner.Run(context.Background(), cfg); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	for _, event := range []string{"run_started", "os_collect_skipped", "db_collect_finished", "manifest_written", "run_finished"} {
		if !hasProgressEvent(events, event) {
			t.Fatalf("expected progress event %s in %+v", event, events)
		}
	}
}

func hasProgressEvent(events []ProgressEvent, event string) bool {
	for _, item := range events {
		if item.Event == event {
			return true
		}
	}
	return false
}

var _ model.Clock = fixedClock{}
