package osinfo

import (
	"context"
	"dbcheck/collector/internal/cli"
)

func collectLocal(_ context.Context, _ cli.Config) (map[string]any, error) {
	payload, err := CollectSinglePayload()
	if err != nil {
		return nil, err
	}
	enrichLocalSystemInfo(payload["system_info"].(map[string]any))
	return payload, nil
}

func buildPayload(s *snapshot, systemInfo map[string]any) map[string]any {
	timed := collectTimedSamples(s)
	payload := map[string]any{
		"system_info": systemInfo,
		"cpu":         map[string]any{"samples": []map[string]any{timed.cpu}},
		"memory":      map[string]any{"samples": []map[string]any{collectMemorySample(s)}},
		"filesystem":  map[string]any{"samples": []map[string]any{collectFilesystemSample(s)}},
		"disk_io":     map[string]any{"samples": []map[string]any{timed.diskIO}},
		"network":     map[string]any{"samples": []map[string]any{timed.network}},
		"process":     map[string]any{"samples": []map[string]any{timed.process}},
	}
	if len(s.errors) > 0 {
		payload["collect_errors"] = s.errors
	}
	return payload
}
