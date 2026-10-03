package core

import (
	"context"
	"dbcheck/collector/internal/cli"
	"testing"
	"time"
)

func TestRunnerRecordsOracleServiceName(t *testing.T) {
	runner, err := NewRunner(Dependencies{
		Clock:       fixedClock{now: time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)},
		DBCollector: stubDBCollector{payload: map[string]any{"basic_info": map[string]any{"version": "19c"}}},
		OSCollector: stubOSCollector{}, Writer: newMemoryWriter(), Version: "1.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := runner.Run(context.Background(), cli.Config{
		DBType: "oracle", DBHost: "10.0.0.1", DBPort: 1521,
		OracleServiceName: " ORCLPDB1 ", OutputDir: "./runs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifacts.Result == nil || artifacts.Result.Meta.DBName != "ORCLPDB1" {
		t.Fatalf("expected service name in collected metadata, got %+v", artifacts.Result)
	}
}
