package osinfo

import (
	"context"
	"dbcheck/collector/internal/cli"
	"fmt"
	"os"
	"runtime"
	"time"
)

func collectLocal(_ context.Context, _ cli.Config) (map[string]any, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("read hostname failed: %w", err)
	}
	s := snapshot{timestamp: time.Now().Format(time.RFC3339), errors: []string{}}
	systemInfo := collectSystemInfo(&s, hostname)
	payload := buildPayload(&s, systemInfo)
	payload["system_info"].(map[string]any)["os"] = runtime.GOOS
	payload["system_info"].(map[string]any)["arch"] = runtime.GOARCH
	payload["system_info"].(map[string]any)["cpu_cores"] = runtime.NumCPU()
	return payload, nil
}

func buildPayload(s *snapshot, systemInfo map[string]any) map[string]any {
	payload := map[string]any{
		"system_info": systemInfo,
		"cpu":         map[string]any{"samples": []map[string]any{collectCPUSample(s)}},
		"memory":      map[string]any{"samples": []map[string]any{collectMemorySample(s)}},
		"filesystem":  map[string]any{"samples": []map[string]any{collectFilesystemSample(s)}},
		"disk_io":     map[string]any{"samples": []map[string]any{collectDiskIOSample(s)}},
		"network":     map[string]any{"samples": []map[string]any{collectNetworkSample(s)}},
		"process":     map[string]any{"samples": []map[string]any{collectProcessSample(s)}},
	}
	if len(s.errors) > 0 {
		payload["collect_errors"] = s.errors
	}
	return payload
}
