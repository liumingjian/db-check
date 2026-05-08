package gaussdb

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"encoding/json"
	"testing"
	"time"
)

type memoryWriter struct {
	files map[string][]byte
}

func newMemoryWriter() *memoryWriter {
	return &memoryWriter{files: map[string][]byte{}}
}

func (w *memoryWriter) PrepareRunDir(outputDir string, runID string) (string, error) {
	return outputDir + "/" + runID, nil
}

func (w *memoryWriter) WriteJSON(path string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.files[path] = payload
	return nil
}

func (w *memoryWriter) WriteText(path string, content string) error {
	w.files[path] = []byte(content)
	return nil
}

func TestCollectorBuildsSQLFirstPayload(t *testing.T) {
	cfg := cli.Config{GaussUser: "Ruby", GaussEnvFile: "~/gauss_env_file"}
	writer := newMemoryWriter()
	collector := Collector{
		CollectSQL: func(context.Context, cli.Config, string, core.ArtifactWriter) (sqlCollectionResult, error) {
			return sqlCollectionResult{
				Metadata: map[string]any{"gaussdb_version": "GaussDB Kernel 505.2.1", "version": "505.2.1"},
				Records: []itemRecord{
					normalRecord(itemSpec{Name: "CheckDBConnection", Domain: "basic_info", Label: "数据库连接"}, "Database connection is normal.", map[string]any{}, 10),
					notApplicableRecord(itemSpec{Name: "CheckErrorInLog", Domain: "performance", Label: "运行日志"}),
				},
				Domains: map[string]sqlDomainExtra{
					"sql_analysis": {
						Summary: map[string]any{"no_statistics_table_count": 7},
						Fields: map[string]any{
							"no_statistics_summary": rowsPayload([]map[string]any{{"tableowner": "rdsAdmin", "table_no_stat": 7}}),
						},
					},
				},
				RawIndex: []map[string]any{{"item": "NoStatisticsSummary"}},
				Errors:   []string{"GaussDB SQL-first mode ignores --gauss-user, --gauss-env-file"},
			}, nil
		},
		Now: func() time.Time { return time.Date(2026, 3, 12, 18, 0, 0, 0, time.UTC) },
	}
	payload, err := collector.Collect(context.Background(), cfg, "/tmp/run", writer)
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if _, ok := payload["gs_check_raw_index"]; ok {
		t.Fatalf("did not expect gs_check raw index in SQL-first mode")
	}
	basic := payload["basic_info"].(map[string]any)
	summary := basic["summary"].(map[string]any)
	if summary["checkdbconnection_status"] != "normal" {
		t.Fatalf("unexpected connection status: %v", summary["checkdbconnection_status"])
	}
	sqlAnalysis := payload["sql_analysis"].(map[string]any)
	sqlSummary := sqlAnalysis["summary"].(map[string]any)
	if sqlSummary["no_statistics_table_count"] != 7 {
		t.Fatalf("unexpected sql summary count: %v", sqlSummary["no_statistics_table_count"])
	}
	if _, ok := payload["sql_raw_index"].(map[string]any); !ok {
		t.Fatalf("expected sql raw index")
	}
	errors, ok := payload["collect_errors"].([]string)
	if !ok || len(errors) != 1 {
		t.Fatalf("expected collect errors, got %#v", payload["collect_errors"])
	}
}

func TestSQLFailureProducesAbnormalRecord(t *testing.T) {
	item := itemSpec{Name: "CheckDBConnection", Domain: "basic_info", Label: "数据库连接"}
	record := failedSQLRecord(item, "select 1", 25, context.DeadlineExceeded)
	if record.NormalizedStatus != "abnormal" {
		t.Fatalf("expected abnormal, got %s", record.NormalizedStatus)
	}
	if record.Details["error"] == "" {
		t.Fatalf("expected error detail")
	}
}

var _ core.ArtifactWriter = (*memoryWriter)(nil)
