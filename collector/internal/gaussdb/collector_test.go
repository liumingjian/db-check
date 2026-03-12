package gaussdb

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type fakeRunner struct {
	outputs map[string]string
}

func (f *fakeRunner) Run(command string) (string, error) {
	output, ok := f.outputs[command]
	if !ok {
		return "", fmt.Errorf("unexpected command: %s", command)
	}
	return output, nil
}

func (f *fakeRunner) Close() error {
	return nil
}

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

func TestParseOutputExtractsStatusSummaryAndRaw(t *testing.T) {
	output := "2026-03-12 18:14:09 [NAM] CheckDBConnection\n2026-03-12 18:14:09 [STD]\n2026-03-12 18:14:09 [RST] OK\nThe database connection is normal.\n2026-03-12 18:14:09 [RAW]\nsource '/home/Ruby/gauss_env_file' && gsql -d postgres\n"
	parsed := parseOutput(output)
	if parsed.Name != "CheckDBConnection" {
		t.Fatalf("unexpected name: %s", parsed.Name)
	}
	if parsed.Status != statusOK {
		t.Fatalf("unexpected status: %s", parsed.Status)
	}
	if parsed.Summary != "The database connection is normal." {
		t.Fatalf("unexpected summary: %s", parsed.Summary)
	}
	if parsed.Raw == "" {
		t.Fatalf("expected raw section")
	}
}

func TestBuildItemCommandUsesSuAndEnvFile(t *testing.T) {
	cfg := cli.Config{GaussUser: "Ruby", GaussEnvFile: "~/gauss_env_file"}
	command := buildItemCommand(cfg, "CheckDBConnection")
	for _, token := range []string{"su - 'Ruby'", `"$HOME/gauss_env_file"`, `gs_check -i "CheckDBConnection" -L`} {
		if !strings.Contains(command, token) {
			t.Fatalf("expected command to contain %s, got %s", token, command)
		}
	}
}

func TestBuildItemCommandUsesActiveLogScanForErrorLogs(t *testing.T) {
	cfg := cli.Config{GaussUser: "Ruby", GaussEnvFile: "~/gauss_env_file"}
	command := buildItemCommand(cfg, "CheckErrorInLog")
	for _, token := range []string{"su - 'Ruby'", `"$HOME/gauss_env_file"`, "lsof", "CheckErrorInLog", "active_log_scan"} {
		if !strings.Contains(command, token) {
			t.Fatalf("expected command to contain %s, got %s", token, command)
		}
	}
	if strings.Contains(command, `gs_check -i "CheckErrorInLog" -L`) {
		t.Fatalf("expected CheckErrorInLog to use active log scan, got %s", command)
	}
}

func TestBuildItemCommandRestoresRemoteHomeWhenShellExpandedTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir failed: %v", err)
	}
	cfg := cli.Config{GaussUser: "Ruby", GaussEnvFile: home + "/gauss_env_file"}
	command := buildItemCommand(cfg, "CheckDBConnection")
	if !strings.Contains(command, `"$HOME/gauss_env_file"`) {
		t.Fatalf("expected remote home expansion, got %s", command)
	}
	if strings.Contains(command, home+"/gauss_env_file") {
		t.Fatalf("expected local home path to be normalized, got %s", command)
	}
}

func TestParseGUCConsistencyDetailsBuildsGroupedParameters(t *testing.T) {
	summary := `{"CN_5001":{"max_connections":"400","shared_buffers":"1GB","checkpoint_timeout":"900","ssl":"on","sql_compatibility":"A"},"DN_6001":{"max_connections":"3000","shared_buffers":"1GB","checkpoint_timeout":"900","ssl":"on","sql_compatibility":"A"}}`
	details := parseGUCConsistencyDetails(summary)
	if details["instance_count"] != 2 {
		t.Fatalf("unexpected instance count: %v", details["instance_count"])
	}
	groups, ok := details["key_groups"].([]map[string]any)
	if !ok || len(groups) == 0 {
		t.Fatalf("expected grouped parameters, got %#v", details["key_groups"])
	}
	firstGroup := groups[0]
	parameters, ok := firstGroup["parameters"].([]map[string]any)
	if !ok || len(parameters) == 0 {
		t.Fatalf("expected parameter rows, got %#v", firstGroup["parameters"])
	}
	differences, ok := details["key_inconsistencies"].([]map[string]any)
	if !ok || len(differences) != 1 {
		t.Fatalf("expected one inconsistency, got %#v", details["key_inconsistencies"])
	}
	if differences[0]["parameter"] != "max_connections" {
		t.Fatalf("unexpected inconsistent parameter: %#v", differences[0])
	}
}

func TestCollectorWritesRawFilesAndIndex(t *testing.T) {
	cfg := cli.Config{GaussUser: "Ruby", GaussEnvFile: "~/gauss_env_file"}
	writer := newMemoryWriter()
	runner := &fakeRunner{outputs: map[string]string{}}
	runner.outputs[buildMetadataCommand(cfg)] = "__DBCHECK_META_BEGIN__\ngaussdb version\n__DBCHECK_SPLIT__\ngsql version\n__DBCHECK_SPLIT__\ngs_check version\n__DBCHECK_SPLIT__\nPGUSER=rdsAdmin\n"
	for _, item := range itemCatalog {
		summary := fmt.Sprintf("%s passed.", item.Name)
		if item.Name == "CheckGUCConsistent" {
			summary = `{"CN_5001":{"max_connections":"400","shared_buffers":"1GB"},"DN_6001":{"max_connections":"3000","shared_buffers":"1GB"}}`
		}
		runner.outputs[buildItemCommand(cfg, item.Name)] = fmt.Sprintf("2026-03-12 18:14:09 [NAM] %s\n2026-03-12 18:14:09 [STD]\n2026-03-12 18:14:09 [RST] OK\n%s\n2026-03-12 18:14:09 [RAW]\nraw output\n", item.Name, summary)
	}
	collector := Collector{
		NewRunner: func(cli.Config) (remoteRunner, error) { return runner, nil },
		CollectSQL: func(context.Context, cli.Config, string, core.ArtifactWriter) (sqlCollectionResult, error) {
			return sqlCollectionResult{
				Domains: map[string]sqlDomainExtra{
					"sql_analysis": {
						Summary: map[string]any{"no_statistics_table_count": 7},
						Fields: map[string]any{
							"no_statistics_summary": rowsPayload([]map[string]any{{"tableowner": "rdsAdmin", "table_no_stat": 7}}),
						},
					},
				},
				RawIndex: []map[string]any{{"item": "NoStatisticsSummary"}},
			}, nil
		},
		Now: func() time.Time { return time.Date(2026, 3, 12, 18, 0, 0, 0, time.UTC) },
	}
	payload, err := collector.Collect(context.Background(), cfg, "/tmp/run", writer)
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if _, ok := writer.files["/tmp/run/gs_check/CheckDBConnection.stdout"]; !ok {
		t.Fatalf("expected raw stdout file")
	}
	if _, ok := writer.files["/tmp/run/gs_check/index.json"]; !ok {
		t.Fatalf("expected raw index file")
	}
	index := payload["gs_check_raw_index"].(map[string]any)
	if index["count"].(int) != len(itemCatalog) {
		t.Fatalf("unexpected raw index count: %v", index["count"])
	}
	basic := payload["basic_info"].(map[string]any)
	summary := basic["summary"].(map[string]any)
	if summary["gauss_user"] != "Ruby" {
		t.Fatalf("unexpected gauss user metadata: %v", summary["gauss_user"])
	}
	config := payload["config_check"].(map[string]any)
	configSummary := config["summary"].(map[string]any)
	gucDetails, ok := configSummary["checkgucconsistent_details"].(map[string]any)
	if !ok {
		t.Fatalf("expected guc structured details")
	}
	if gucDetails["key_inconsistent_parameter_count"] != 1 {
		t.Fatalf("unexpected inconsistency count: %v", gucDetails["key_inconsistent_parameter_count"])
	}
	sqlAnalysis := payload["sql_analysis"].(map[string]any)
	sqlSummary := sqlAnalysis["summary"].(map[string]any)
	if sqlSummary["no_statistics_table_count"] != 7 {
		t.Fatalf("unexpected sql summary count: %v", sqlSummary["no_statistics_table_count"])
	}
	if _, ok := payload["sql_raw_index"].(map[string]any); !ok {
		t.Fatalf("expected sql raw index")
	}
}

var _ core.ArtifactWriter = (*memoryWriter)(nil)
