package osinfo

import (
	"context"
	"dbcheck/collector/internal/cli"
	"fmt"
	"testing"
)

type stubRemoteRunner struct {
	outputs map[string]string
}

func (s stubRemoteRunner) Run(command string) (string, error) {
	value, ok := s.outputs[command]
	if !ok {
		return "", fmt.Errorf("unexpected command: %s", command)
	}
	return value, nil
}

func (s stubRemoteRunner) Close() error { return nil }

func TestCollectorRemoteLinuxPayload(t *testing.T) {
	outputs := map[string]string{
		remoteHostnameCommand:     "os-target\n",
		remoteKernelCommand:       "Linux\n",
		remoteArchCommand:         "x86_64\n",
		remoteCPUCoreCommand:      "4\n",
		remoteFDUsageCommand:      "1.25\n",
		remoteMySQLFDUsageCommand: "0\n",
		remoteTHPCommand:          "always madvise [never]\n",
		remoteCPUCommand:          "12.500000\t0.500000\n",
		remoteMemInfoCommand:      "MemTotal: 1024 kB\nMemAvailable: 512 kB\nSwapTotal: 256 kB\nSwapFree: 128 kB\n",
		remoteFilesystemCommand:   "/dev/sda1\text4\t100\t40\t40%\t/\n",
		remoteUptimeCommand:       "1000.00 10.0\n",
		remoteDiskstatsCommand:    "8 0 sda 10 0 20 30 40 0 50 60 0 70 80 0 0 0 0 0 0\n",
		remoteNetDevCommand:       "Inter-|   Receive                                                |  Transmit\n eth0: 100 0 1 0 0 0 0 0 200 0 2 0 0 0 0 0\n",
		remoteLoadAvgCommand:      "0.25 0.10 0.05 1/100 123\n",
	}
	collector := Collector{
		NewRemoteRunner: func(cfg cli.Config) (remoteRunner, error) {
			if !cfg.UseRemoteOS {
				t.Fatalf("expected remote OS collection to be enabled")
			}
			return stubRemoteRunner{outputs: outputs}, nil
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
	if systemInfo["os"] != "linux" {
		t.Fatalf("unexpected os: %#v", systemInfo["os"])
	}
	if systemInfo["arch"] != "amd64" {
		t.Fatalf("unexpected arch: %#v", systemInfo["arch"])
	}
	cpu := payload["cpu"].(map[string]any)["samples"].([]map[string]any)
	if cpu[0]["usage_percent"].(float64) <= 0 {
		t.Fatalf("expected cpu usage > 0")
	}
	mounts := payload["filesystem"].(map[string]any)["samples"].([]map[string]any)[0]["mountpoints"].([]map[string]any)
	if len(mounts) != 1 || mounts[0]["mountpoint"] != "/" {
		t.Fatalf("unexpected mountpoints: %#v", mounts)
	}
}
