package osinfo

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/osprobeassets"
	"encoding/json"
	"fmt"
	"time"
)

var sampledPayloadKeys = [...]string{"cpu", "memory", "filesystem", "disk_io", "network", "process"}

func (c Collector) collectRemote(_ context.Context, cfg cli.Config) (map[string]any, error) {
	runnerFactory := c.NewRemoteRunner
	if runnerFactory == nil {
		runnerFactory = newSSHRunner
	}
	runner, err := runnerFactory(cfg)
	if err != nil {
		return nil, err
	}
	defer runner.Close()

	platform, err := detectRemotePlatform(runner)
	if err != nil {
		return nil, err
	}
	asset, err := osprobeassets.Lookup(platform.GOOS, platform.GOARCH)
	if err != nil {
		return nil, err
	}
	remotePath, err := runner.UploadExecutable("db-osprobe", asset)
	if err != nil {
		return nil, fmt.Errorf("upload remote os probe failed: %w", err)
	}
	defer runner.Remove(remotePath)

	output, err := runner.RunExecutable(remotePath)
	if err != nil {
		return nil, fmt.Errorf("run remote os probe failed: %w", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(output), &payload); err != nil {
		return nil, fmt.Errorf("decode remote os probe payload failed: %w", err)
	}
	normalizeRemotePayloadTimestamps(payload, time.Now())
	return payload, nil
}

func normalizeRemotePayloadTimestamps(payload map[string]any, capturedAt time.Time) {
	timestamp := capturedAt.Format(time.RFC3339)
	for _, key := range sampledPayloadKeys {
		normalizeSampleTimestamp(payload, key, timestamp)
	}
}

func normalizeSampleTimestamp(payload map[string]any, key string, timestamp string) {
	section, ok := payload[key].(map[string]any)
	if !ok {
		return
	}
	samples, ok := section["samples"].([]any)
	if !ok {
		return
	}
	for _, item := range samples {
		sample, ok := item.(map[string]any)
		if !ok {
			continue
		}
		sample["timestamp"] = timestamp
	}
}
