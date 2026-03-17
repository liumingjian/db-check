package web

import (
	"dbcheck/reporter/internal/launcher"
	"errors"
	"path/filepath"
	"testing"
)

func TestPipelineContinuesOnItemFailureAndStreamsLogs(t *testing.T) {
	root := t.TempDir()
	p := NewPipeline("ignored", "python3")

	// Stub dependencies to keep the test hermetic.
	p.ExtractZip = func(zipPath string, destDir string) error { return nil }
	p.DetectRun = func(root string) (string, error) { return filepath.Join(root, "run"), nil }
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
	calls   int
	results []error
}

func (r *fakeRunner) Run(command string, args []string, onLog func(LogEvent)) error {
	r.calls++
	if onLog != nil {
		onLog(LogEvent{Stream: LogStdout, Line: "hello"})
		onLog(LogEvent{Stream: LogStderr, Line: "world"})
	}
	if r.calls-1 < len(r.results) {
		return r.results[r.calls-1]
	}
	return nil
}
