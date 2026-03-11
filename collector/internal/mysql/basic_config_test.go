package mysql

import (
	"context"
	"reflect"
	"testing"
)

func TestCollectVersionInfo(t *testing.T) {
	collector := &metricsCollector{
		varCache: map[string]string{
			"version":                 "8.0.26",
			"version_comment":         "MySQL Community Server - GPL",
			"version_compile_os":      "Linux",
			"version_compile_machine": "x86_64",
		},
	}

	got := collector.collectVersionInfo(context.Background())
	want := map[string]any{
		"version":         "8.0.26",
		"version_comment": "MySQL Community Server - GPL",
		"version_vars": map[string]any{
			"version":                 "8.0.26",
			"version_compile_os":      "Linux",
			"version_compile_machine": "x86_64",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collectVersionInfo() mismatch:\nwant=%#v\ngot=%#v", want, got)
	}
}

func TestCollectConfigCheckIncludesFileLocationVariables(t *testing.T) {
	collector := &metricsCollector{
		varCache: map[string]string{
			"datadir":               "/data/mysql/",
			"socket":                "/data/mysql/mysql.sock",
			"log_error":             "/data/mysql/logs/error.log",
			"transaction_isolation": "REPEATABLE-READ",
		},
	}

	config := collector.collectConfigCheck(context.Background())
	if got := config["datadir"]; got != "/data/mysql/" {
		t.Fatalf("datadir mismatch: got=%v", got)
	}
	if got := config["socket"]; got != "/data/mysql/mysql.sock" {
		t.Fatalf("socket mismatch: got=%v", got)
	}
	if got := config["log_error"]; got != "/data/mysql/logs/error.log" {
		t.Fatalf("log_error mismatch: got=%v", got)
	}
}
