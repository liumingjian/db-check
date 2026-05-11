package web

import (
	"dbcheck/reporter/internal/launcher"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPipelineContinuesOnItemFailureAndStreamsLogs(t *testing.T) {
	root := t.TempDir()
	p := NewPipeline("ignored", "python3")

	// Stub dependencies to keep the test hermetic.
	p.ExtractZip = func(zipPath string, destDir string) error { return nil }
	p.DetectRun = func(root string) (string, error) { return writeRunDir(t, root, "mysql"), nil }
	p.LayoutResolver = fakeLayoutResolver{}

	runner := &fakeRunner{
		results: []error{
			errors.New("boom"),
			nil,
		},
	}
	p.Runner = runner

	var gotLogs []string
	results := p.RunItems(root, []ItemInput{
		{ID: "i1", ZipPath: "/tmp/a.zip"},
		{ID: "i2", ZipPath: "/tmp/b.zip"},
	}, func(itemID string, ev LogEvent) {
		gotLogs = append(gotLogs, itemID+":"+string(ev.Stream)+":"+ev.Line)
	})

	if runner.calls != 2 {
		t.Fatalf("expected runner calls=2 got %d", runner.calls)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results got %d", len(results))
	}
	if results[0].ID != "i1" || results[0].Status != ItemFailed {
		t.Fatalf("unexpected result[0]: %#v", results[0])
	}
	if results[1].ID != "i2" || results[1].Status != ItemDone {
		t.Fatalf("unexpected result[1]: %#v", results[1])
	}
	if len(gotLogs) == 0 {
		t.Fatalf("expected some logs, got none")
	}
}

func TestPipelinePassesWDRFileForGaussDB(t *testing.T) {
	root := t.TempDir()
	p := NewPipeline("ignored", "python3")
	p.ExtractZip = func(zipPath string, destDir string) error { return nil }
	p.DetectRun = func(root string) (string, error) {
		return writeRunDir(t, root, "gaussdb"), nil
	}
	p.LayoutResolver = fakeLayoutResolver{}
	runner := &fakeRunner{}
	p.Runner = runner

	results := p.RunItems(root, []ItemInput{{ID: "i1", ZipPath: "/tmp/a.zip", WDRPath: "/tmp/wdr.html"}}, nil)
	if results[0].Status != ItemDone {
		t.Fatalf("expected done: %#v", results[0])
	}
	if !hasArg(runner.lastArgs, "--wdr-file", "/tmp/wdr.html") {
		t.Fatalf("expected --wdr-file in args: %#v", runner.lastArgs)
	}
}

func TestPipelinePassesAWRFileForOracle(t *testing.T) {
	root := t.TempDir()
	p := NewPipeline("ignored", "python3")
	p.ExtractZip = func(zipPath string, destDir string) error { return nil }
	p.DetectRun = func(root string) (string, error) {
		return writeRunDir(t, root, "oracle"), nil
	}
	p.LayoutResolver = fakeLayoutResolver{}
	runner := &fakeRunner{}
	p.Runner = runner

	results := p.RunItems(root, []ItemInput{{ID: "i1", ZipPath: "/tmp/a.zip", AWRPath: "/tmp/awr.html"}}, nil)
	if results[0].Status != ItemDone {
		t.Fatalf("expected done: %#v", results[0])
	}
	if !hasArg(runner.lastArgs, "--awr-file", "/tmp/awr.html") {
		t.Fatalf("expected --awr-file in args: %#v", runner.lastArgs)
	}
}

func TestPipelineRejectsMismatchedHTMLAttachments(t *testing.T) {
	cases := []struct {
		name    string
		dbType  string
		item    ItemInput
		wantErr string
	}{
		{name: "oracle wdr", dbType: "oracle", item: ItemInput{ID: "i1", ZipPath: "/tmp/a.zip", WDRPath: "/tmp/wdr.html"}, wantErr: "wdr file is only supported for GaussDB"},
		{name: "gaussdb awr", dbType: "gaussdb", item: ItemInput{ID: "i1", ZipPath: "/tmp/a.zip", AWRPath: "/tmp/awr.html"}, wantErr: "awr file is only supported for Oracle"},
		{name: "mysql awr", dbType: "mysql", item: ItemInput{ID: "i1", ZipPath: "/tmp/a.zip", AWRPath: "/tmp/awr.html"}, wantErr: "AWR/WDR HTML is not supported for MySQL"},
		{name: "mysql wdr", dbType: "mysql", item: ItemInput{ID: "i1", ZipPath: "/tmp/a.zip", WDRPath: "/tmp/wdr.html"}, wantErr: "AWR/WDR HTML is not supported for MySQL"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			p := NewPipeline("ignored", "python3")
			p.ExtractZip = func(zipPath string, destDir string) error { return nil }
			p.DetectRun = func(root string) (string, error) {
				return writeRunDir(t, root, tc.dbType), nil
			}
			p.LayoutResolver = fakeLayoutResolver{}
			p.Runner = &fakeRunner{}

			results := p.RunItems(root, []ItemInput{tc.item}, nil)
			if results[0].Status != ItemFailed {
				t.Fatalf("expected failed: %#v", results[0])
			}
			if !strings.Contains(results[0].Error, tc.wantErr) {
				t.Fatalf("expected %q in %q", tc.wantErr, results[0].Error)
			}
		})
	}
}

func writeRunDir(t *testing.T, root string, dbType string) string {
	t.Helper()
	runDir := filepath.Join(root, "run")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir failed: %v", err)
	}
	content := []byte(`{"db_type":"` + dbType + `"}`)
	if err := os.WriteFile(filepath.Join(runDir, "manifest.json"), content, 0o644); err != nil {
		t.Fatalf("write manifest failed: %v", err)
	}
	return runDir
}

func hasArg(args []string, flag string, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

type fakeLayoutResolver struct{}

func (fakeLayoutResolver) Resolve(executablePath string, cfg launcher.Config) (launcher.AssetLayout, error) {
	return launcher.AssetLayout{
		Script:       "script.py",
		RuleFile:     "rule.json",
		TemplateFile: "template.docx",
		Requirements: "requirements.txt",
	}, nil
}

type fakeRunner struct {
	calls    int
	results  []error
	lastArgs []string
}

func (r *fakeRunner) Run(command string, args []string, onLog func(LogEvent)) error {
	r.calls++
	r.lastArgs = append([]string{}, args...)
	if onLog != nil {
		onLog(LogEvent{Stream: LogStdout, Line: "hello"})
		onLog(LogEvent{Stream: LogStderr, Line: "world"})
	}
	if r.calls-1 < len(r.results) {
		return r.results[r.calls-1]
	}
	return nil
}
