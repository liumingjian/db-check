package osinfo

import (
	"context"
	"dbcheck/collector/internal/cli"
	"fmt"
	"testing"
	"time"
)

type stubRemoteRunner struct {
	commands   map[string]string
	executable string
	removed    string
}

func (s *stubRemoteRunner) Run(command string) (string, error) {
	value, ok := s.commands[command]
	if !ok {
		return "", fmt.Errorf("unexpected command: %s", command)
	}
	return value, nil
}

func (s *stubRemoteRunner) UploadExecutable(name string, content []byte) (string, error) {
	if name != "db-osprobe" {
		return "", fmt.Errorf("unexpected executable name: %s", name)
	}
	if len(content) == 0 {
		return "", fmt.Errorf("empty executable content")
	}
	s.executable = "/tmp/db-osprobe-test"
	return s.executable, nil
}

func (s *stubRemoteRunner) RunExecutable(path string) (string, error) {
	if path != s.executable {
		return "", fmt.Errorf("unexpected executable path: %s", path)
	}
	return `{"system_info":{"hostname":"os-target","os":"linux","arch":"amd64","cpu_cores":4},"cpu":{"samples":[{"usage_percent":12.5}]}}`, nil
}

func (s *stubRemoteRunner) Remove(path string) error {
	s.removed = path
	return nil
}

func (s *stubRemoteRunner) Close() error { return nil }

func TestCollectorRemotePayloadUsesEmbeddedProbe(t *testing.T) {
	runner := &stubRemoteRunner{
		commands: map[string]string{
			remoteKernelCommand: "Linux\n",
			remoteArchCommand:   "x86_64\n",
		},
	}
	collector := Collector{
		NewRemoteRunner: func(cfg cli.Config) (remoteRunner, error) {
			if !cfg.UseRemoteOS {
				t.Fatalf("expected remote OS collection to be enabled")
			}
			return runner, nil
		},
	}
	payload, err := collector.Collect(context.Background(), cli.Config{UseRemoteOS: true})
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	systemInfo := payload["system_info"].(map[string]any)
	if systemInfo["hostname"] != "os-target" {
		t.Fatalf("unexpected hostname: %#v", systemInfo["hostname"])
	}
	if runner.removed != runner.executable {
		t.Fatalf("expected uploaded executable to be removed: %#v", runner.removed)
	}
}

func TestNormalizeRemotePayloadTimestampsUsesCollectorClock(t *testing.T) {
	payload := map[string]any{
		"cpu":        map[string]any{"samples": []any{map[string]any{"timestamp": "2026-03-12T12:31:31+08:00"}}},
		"memory":     map[string]any{"samples": []any{map[string]any{}}},
		"filesystem": map[string]any{"samples": []any{map[string]any{"timestamp": "remote"}}},
		"disk_io":    map[string]any{"samples": []any{map[string]any{"timestamp": "remote"}}},
		"network":    map[string]any{"samples": []any{map[string]any{"timestamp": "remote"}}},
		"process":    map[string]any{"samples": []any{map[string]any{"timestamp": "remote"}}},
	}
	capturedAt := time.Date(2026, 3, 12, 12, 25, 13, 0, time.FixedZone("CST", 8*3600))

	normalizeRemotePayloadTimestamps(payload, capturedAt)

	expected := capturedAt.Format(time.RFC3339)
	for _, key := range sampledPayloadKeys {
		section := payload[key].(map[string]any)
		sample := section["samples"].([]any)[0].(map[string]any)
		if sample["timestamp"] != expected {
			t.Fatalf("unexpected timestamp for %s: %#v", key, sample["timestamp"])
		}
	}
}
