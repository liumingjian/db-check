package web

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"dbcheck/reporter/internal/reports"
	"dbcheck/reporter/internal/users"

	"nhooyr.io/websocket"
)

// fakePipeline stands in for the report pipeline. Items whose file name
// contains "fail" fail; the others get a report document. When gate is set,
// every item waits for it to close.
type fakePipeline struct {
	gate chan struct{}

	mu   sync.Mutex
	jobs []ItemJob
}

func (p *fakePipeline) RunItem(ctx context.Context, job ItemJob, onLog func(LogEvent)) ItemResult {
	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return ItemResult{ID: job.Input.ID, Status: ItemFailed, Error: ctx.Err().Error()}
		}
	}
	p.mu.Lock()
	p.jobs = append(p.jobs, job)
	p.mu.Unlock()
	onLog(LogEvent{Stream: LogStdout, Line: "rendering"})
	if strings.Contains(job.Input.Name, "fail") {
		return ItemResult{ID: job.Input.ID, Status: ItemFailed, Error: "模拟失败"}
	}
	path := ItemReportPath(job.TaskDir, job.Input.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return ItemResult{ID: job.Input.ID, Status: ItemFailed, Error: err.Error()}
	}
	if err := os.WriteFile(path, []byte("docx"), 0o644); err != nil {
		return ItemResult{ID: job.Input.ID, Status: ItemFailed, Error: err.Error()}
	}
	return ItemResult{ID: job.Input.ID, Status: ItemDone, ReportDocx: path}
}

// ran lists "<task>/<item id>" for every item the pipeline ran.
func (p *fakePipeline) ran() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.jobs))
	for _, j := range p.jobs {
		out = append(out, j.TaskID+"/"+j.Input.ID)
	}
	return out
}

// reportsFixture is a data directory and store with three active users
// (engineers "user" and "other", admin "admin"). Each handler it builds is
// one run of the server over that data directory.
type reportsFixture struct {
	*platformFixture
	cfg      Config
	platform Platform
}

func newReportsFixture(t *testing.T) *reportsFixture {
	t.Helper()
	pf := newPlatformFixture(t)
	pf.addUser("user", users.RoleEngineer, users.StatusActive)
	pf.addUser("other", users.RoleEngineer, users.StatusActive)
	pf.addUser("admin", users.RoleAdmin, users.StatusActive)
	return &reportsFixture{platformFixture: pf, cfg: pf.api.cfg, platform: pf.api.platform}
}

// start runs the server again over the same data directory, this time with
// the worker running on pipeline: a restart.
func (f *reportsFixture) start(pipeline ReportPipeline) http.Handler {
	f.t.Helper()
	p := f.platform
	p.Pipeline = pipeline
	h, err := newAPIHandler(f.cfg, p, false)
	if err != nil {
		f.t.Fatalf("newAPIHandler: %v", err)
	}
	f.startWorker(h.reports)
	return h.handler()
}

// upload is one report item to submit: a collector ZIP with this name, db
// type, and collector version (empty for none).
type upload struct {
	name, dbType, version string
}

func collectorZipBytes(t *testing.T, dbType, version string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	files := map[string]string{
		"run/manifest.json": fmt.Sprintf(`{"db_type":%q,"artifacts":{"result":"result.json"}}`, dbType),
		"run/result.json":   fmt.Sprintf(`{"meta":{"collector_version":%q}}`, version),
	}
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(w, content); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type formFile struct {
	field, name string
	content     []byte
}

func generateRequest(t *testing.T, token string, files []formFile) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, f := range files {
		part, err := mw.CreateFormFile(f.field, f.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(f.content); err != nil {
			t.Fatal(err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/reports/generate", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func zipsOf(t *testing.T, uploads ...upload) []formFile {
	t.Helper()
	files := make([]formFile, 0, len(uploads))
	for _, u := range uploads {
		files = append(files, formFile{"zips", u.name, collectorZipBytes(t, u.dbType, u.version)})
	}
	return files
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// generate submits a report task and returns its id.
func (f *reportsFixture) generate(h http.Handler, token string, uploads ...upload) string {
	f.t.Helper()
	rec := serve(h, generateRequest(f.t, token, zipsOf(f.t, uploads...)))
	if rec.Code != http.StatusOK {
		f.t.Fatalf("generate: %d %s", rec.Code, rec.Body)
	}
	var body struct {
		TaskID string `json:"task_id"`
		Status string
		Total  int
		WsURL  string `json:"ws_url"`
	}
	decode(f.t, rec, &body)
	if body.Status != "processing" || body.Total != len(uploads) || body.WsURL != "/api/reports/ws/"+body.TaskID {
		f.t.Fatalf("generate answered %+v", body)
	}
	return body.TaskID
}

func get(h http.Handler, path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return serve(h, req)
}

type statusBody struct {
	Status      string
	Total       int
	Completed   int
	DownloadURL string `json:"download_url"`
}

func (f *reportsFixture) status(h http.Handler, token, taskID string) statusBody {
	f.t.Helper()
	rec := get(h, "/api/reports/status/"+taskID, token)
	if rec.Code != http.StatusOK {
		f.t.Fatalf("status %s: %d %s", taskID, rec.Code, rec.Body)
	}
	var s statusBody
	decode(f.t, rec, &s)
	return s
}

// waitFinished polls the task's status until it is done or failed.
func (f *reportsFixture) waitFinished(h http.Handler, token, taskID string) statusBody {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := f.status(h, token, taskID)
		if s.Status == "done" || s.Status == "failed" {
			return s
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("task %s still %+v", taskID, s)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// reportEntries lists the entries of a task's downloaded result ZIP.
func (f *reportsFixture) reportEntries(h http.Handler, token, taskID string) []string {
	f.t.Helper()
	rec := get(h, "/api/reports/download/"+taskID, token)
	if rec.Code != http.StatusOK {
		f.t.Fatalf("download %s: %d %s", taskID, rec.Code, rec.Body)
	}
	zr, err := zip.NewReader(bytes.NewReader(rec.Body.Bytes()), int64(rec.Body.Len()))
	if err != nil {
		f.t.Fatalf("download is not a ZIP: %v", err)
	}
	var names []string
	for _, file := range zr.File {
		names = append(names, file.Name)
	}
	sort.Strings(names)
	return names
}

// dialWS opens the task's WebSocket with token as the subprotocol.
func dialWS(t *testing.T, srv *httptest.Server, taskID, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/reports/ws/" + taskID
	return websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{token},
		HTTPHeader:   http.Header{"Origin": []string{"http://example.com"}},
	})
}

// Tests own the worker lifetime, so it cannot write after the store or data
// directory cleanup. Retention runs once; no hourly test goroutine is needed.
func (f *reportsFixture) startWorker(l *reportLifecycle) {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := reports.RequeueInterrupted(ctx, l.platform.DB); err != nil {
		cancel()
		f.t.Fatal(err)
	}
	if err := l.removeExpiredFiles(ctx); err != nil {
		cancel()
		f.t.Fatal(err)
	}
	if _, err := cleanupExpiredTasks(l.legacy, l.cfg.RetentionTTL, l.platform.Now); err != nil {
		cancel()
		f.t.Fatal(err)
	}
	legacy := l.legacyBacklog()
	done := make(chan struct{})
	go func() { defer close(done); l.work(ctx, legacy) }()
	f.t.Cleanup(func() { cancel(); <-done })
}
