package osinfo

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

func CollectSinglePayload() (map[string]any, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("read hostname failed: %w", err)
	}
	snap := newSnapshot()
	systemInfo := collectSystemInfo(snap, hostname)
	return buildPayload(snap, systemInfo), nil
}

func newSnapshot() *snapshot {
	return &snapshot{
		timestamp: time.Now().Format(time.RFC3339),
		errors:    []string{},
	}
}

func enrichLocalSystemInfo(systemInfo map[string]any) {
	systemInfo["os"] = runtime.GOOS
	systemInfo["arch"] = runtime.GOARCH
	systemInfo["cpu_cores"] = runtime.NumCPU()
}
