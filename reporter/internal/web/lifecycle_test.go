package web

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbcheck/reporter/internal/launcher"
)

func TestTaskLifecycleGetAndWatchReturnsSnapshotAndUpdates(t *testing.T) {
	lifecycle, err := NewTaskLifecycle(Config{DataDir: t.TempDir(), LogReplayLines: 10})
	if err != nil {
		t.Fatalf("NewTaskLifecycle failed: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := lifecycle.Close(ctx); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	if _, err := lifecycle.store.Create(Task{ID: "t1", Status: TaskProcessing, Total: 2, Completed: 1, CurrentFile: "second.zip"}); err != nil {
		t.Fatalf("Create task failed: %v", err)
	}
	lifecycle.hub.emitLog("t1", "info", "first item finished")

	task, err := lifecycle.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if task.Completed != 1 || task.CurrentFile != "second.zip" {
		t.Fatalf("unexpected task snapshot: %#v", task)
	}

	watch, err := lifecycle.Watch(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer watch.Close()
	if watch.Task.ID != "t1" || watch.LastSequence != 1 || len(watch.Logs) != 1 {
		t.Fatalf("unexpected watch snapshot: %#v", watch)
	}

	lifecycle.hub.emitProgress("t1", 1, 2, "second.zip")
	select {
	case update := <-watch.Updates:
		var message map[string]any
		if err := json.Unmarshal(update, &message); err != nil {
			t.Fatalf("decode update failed: %v", err)
		}
		if message["type"] != "progress" || message["current_file"] != "second.zip" {
			t.Fatalf("unexpected update: %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lifecycle update")
	}
}

func TestHTTPSubmissionRunsThroughLifecycleToDownload(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
		MaxUploadBytes: 0,
		PythonBin:      "python3",
		RetentionTTL:   0,
	}
	lifecycle, err := newTaskLifecycle(cfg, func() (*Pipeline, error) {
		return controlledLifecyclePipeline(), nil
	})
	if err != nil {
		t.Fatalf("newTaskLifecycle failed: %v", err)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := lifecycle.Close(ctx); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	handler := newAPIHandlerWithLifecycle(cfg, lifecycle).handler()
	req := multipartRequest(t, map[string]string{"zips": "demo.zip"})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("generate status=%d body=%s", rec.Code, rec.Body.String())
	}

	var submitted struct {
		TaskID string `json:"task_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &submitted); err != nil {
		t.Fatalf("decode submission failed: %v", err)
	}
	if submitted.TaskID == "" || submitted.Status != string(TaskProcessing) {
		t.Fatalf("unexpected submission response: %#v", submitted)
	}

	waitForTaskStatus(t, lifecycle, submitted.TaskID, TaskDone)

	statusReq := httptest.NewRequest(http.MethodGet, "/api/reports/status/"+submitted.TaskID, nil)
	statusReq.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	statusRec := httptest.NewRecorder()
	handler.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status request=%d body=%s", statusRec.Code, statusRec.Body.String())
	}
	var status struct {
		Status      string `json:"status"`
		DownloadURL string `json:"download_url"`
	}
	if err := json.Unmarshal(statusRec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status failed: %v", err)
	}
	if status.Status != string(TaskDone) || status.DownloadURL == "" {
		t.Fatalf("unexpected status response: %#v", status)
	}

	downloadReq := httptest.NewRequest(http.MethodGet, status.DownloadURL, nil)
	downloadReq.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	downloadRec := httptest.NewRecorder()
	handler.ServeHTTP(downloadRec, downloadReq)
	if downloadRec.Code != http.StatusOK {
		t.Fatalf("download status=%d body=%s", downloadRec.Code, downloadRec.Body.String())
	}
	if got := downloadRec.Header().Get("Content-Type"); got != "application/zip" {
		t.Fatalf("unexpected content type: %q", got)
	}
	archive, err := zip.NewReader(bytes.NewReader(downloadRec.Body.Bytes()), int64(downloadRec.Body.Len()))
	if err != nil {
		t.Fatalf("download is not a zip: %v", err)
	}
	if len(archive.File) != 1 || archive.File[0].Name != "demo.zip/report.docx" {
		t.Fatalf("unexpected download contents: %#v", archive.File)
	}
}

func waitForTaskStatus(t *testing.T, lifecycle *TaskLifecycle, taskID string, want TaskStatus) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		task, err := lifecycle.Get(context.Background(), taskID)
		if err != nil {
			t.Fatalf("Get task failed: %v", err)
		}
		if task.Status == want {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("task %s did not reach %q; latest=%#v", taskID, want, task)
		}
	}
}

func controlledLifecyclePipeline() *Pipeline {
	pipeline := NewPipeline("ignored", "python3")
	pipeline.ExtractZip = func(string, string) error { return nil }
	pipeline.DetectRun = func(root string) (string, error) {
		runDir := filepath.Join(root, "run")
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(filepath.Join(runDir, "manifest.json"), []byte(`{"db_type":"mysql"}`), 0o644); err != nil {
			return "", err
		}
		return runDir, nil
	}
	pipeline.LayoutResolver = lifecycleTestLayoutResolver{}
	pipeline.Runner = lifecycleTestRunner{}
	return pipeline
}

type lifecycleTestLayoutResolver struct{}

func (lifecycleTestLayoutResolver) Resolve(string, launcher.Config) (launcher.AssetLayout, error) {
	return launcher.AssetLayout{
		Script:       "script.py",
		RuleFile:     "rule.json",
		TemplateFile: "template.docx",
		Requirements: "requirements.txt",
	}, nil
}

type lifecycleTestRunner struct{}

func (lifecycleTestRunner) Run(_ string, args []string, onLog func(LogEvent)) error {
	if onLog != nil {
		onLog(LogEvent{Stream: LogStdout, Line: "report generated"})
	}
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "--out-docx" {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(args[i+1]), 0o755); err != nil {
			return err
		}
		return os.WriteFile(args[i+1], []byte("controlled report"), 0o644)
	}
	return fmt.Errorf("missing --out-docx argument: %#v", args)
}
