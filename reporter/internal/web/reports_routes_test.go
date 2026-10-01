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

func (p *fakePipeline) RunItem(_ context.Context, job ItemJob, onLog func(LogEvent)) ItemResult {
	if p.gate != nil {
		<-p.gate
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
	h, err := newAPIHandler(f.cfg, p, true)
	if err != nil {
		f.t.Fatalf("newAPIHandler: %v", err)
	}
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

func TestGenerateStoresTheTaskBeforeAcknowledgingAndARestartRunsIt(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"mall.zip", "mysql", "1.2.0"}, upload{"core.zip", "oracle", "1.1.0"})

	// Nothing runs it in this server, yet the task is stored and processing.
	if s := f.status(f.handler, user, taskID); s.Status != "processing" || s.Total != 2 || s.Completed != 0 {
		t.Fatalf("status before any worker = %+v", s)
	}

	pipeline := &fakePipeline{}
	restarted := f.start(pipeline)
	if s := f.waitFinished(restarted, user, taskID); s.Status != "done" || s.Completed != 2 || s.DownloadURL != "/api/reports/download/"+taskID {
		t.Fatalf("status after restart = %+v", s)
	}
	if got := f.reportEntries(restarted, user, taskID); strings.Join(got, ",") != "core.zip/report.docx,mall.zip/report.docx" {
		t.Fatalf("report entries = %v", got)
	}
}

func TestGenerateRecordsTheSubmitterAndWhatTheServerReadFromEachZip(t *testing.T) {
	f := newReportsFixture(t)
	taskID := f.generate(f.handler, f.token("user"),
		upload{"ora.zip", "Oracle", "1.1.0"}, upload{"gauss.zip", "gaussdb", ""})

	task, err := reports.Get(context.Background(), f.db, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.SubmitterID != "u-user" || !task.CreatedAt.Equal(f.clock.Now()) {
		t.Fatalf("task = %+v", task)
	}
	got := make([]string, 0, len(task.Items))
	for _, it := range task.Items {
		version := "<none>"
		if it.CollectorVersion != nil {
			version = *it.CollectorVersion
		}
		got = append(got, fmt.Sprintf("%d %s %s %s %s", it.Position, it.FileName, it.DBType, version, it.Status))
	}
	want := []string{"1 ora.zip oracle 1.1.0 queued", "2 gauss.zip gaussdb <none> queued"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("items = %q, want %q", got, want)
	}
}

func TestGenerateRefusesAZipTheServerCannotRead(t *testing.T) {
	f := newReportsFixture(t)
	files := append(zipsOf(t, upload{"good.zip", "mysql", "1.2.0"}), formFile{"zips", "broken.zip", []byte("not a zip")})
	rec := serve(f.handler, generateRequest(t, f.token("user"), files))
	expectAPIError(t, rec, http.StatusBadRequest, "invalid", "broken.zip：不是有效的 ZIP 文件")

	entries, err := os.ReadDir(filepath.Join(f.cfg.DataDir, "tasks"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("a refused submission left %d task directories", len(entries))
	}
}

func TestGenerateRefusesAnEmptySubmission(t *testing.T) {
	f := newReportsFixture(t)
	expectAPIError(t, serve(f.handler, generateRequest(t, f.token("user"), nil)), http.StatusBadRequest, "invalid", "ZIP")
}

func TestMoreSubmissionsThanTheOldQueueHeldAllFinish(t *testing.T) {
	f := newReportsFixture(t)
	pipeline := &fakePipeline{gate: make(chan struct{})}
	h := f.start(pipeline)
	user := f.token("user")

	// The old in-memory queue held 32 tasks behind the running one and
	// dropped the rest.
	var ids []string
	for i := range 40 {
		ids = append(ids, f.generate(h, user, upload{fmt.Sprintf("db-%02d.zip", i), "mysql", "1.2.0"}))
	}
	close(pipeline.gate)
	for _, id := range ids {
		if s := f.waitFinished(h, user, id); s.Status != "done" {
			t.Fatalf("task %s = %+v", id, s)
		}
	}
	if n := len(pipeline.ran()); n != 40 {
		t.Fatalf("pipeline ran %d items, want 40", n)
	}
}

func TestARestartResumesAnInterruptedTaskWithoutRerunningFinishedItems(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"a.zip", "mysql", "1.2.0"}, upload{"b-fail.zip", "mysql", "1.2.0"}, upload{"c.zip", "mysql", "1.2.0"})

	// A first run finished item 1 and was interrupted on item 2.
	ctx := context.Background()
	if id, ok, err := reports.ClaimNext(ctx, f.db); err != nil || !ok || id != taskID {
		t.Fatalf("ClaimNext = %q %v %v", id, ok, err)
	}
	taskDir := filepath.Join(f.cfg.DataDir, "tasks", taskID)
	first := &fakePipeline{}
	first.RunItem(ctx, ItemJob{TaskID: taskID, TaskDir: taskDir, Input: ItemInput{ID: "1", Name: "a.zip"}}, func(LogEvent) {})
	if err := reports.SetItemStatus(ctx, f.db, taskID, 1, reports.StatusDone, ""); err != nil {
		t.Fatal(err)
	}
	if err := reports.SetItemStatus(ctx, f.db, taskID, 2, reports.StatusProcessing, ""); err != nil {
		t.Fatal(err)
	}

	pipeline := &fakePipeline{}
	h := f.start(pipeline)
	if s := f.waitFinished(h, user, taskID); s.Status != "done" || s.Completed != 3 {
		t.Fatalf("status after restart = %+v", s)
	}
	if got := strings.Join(pipeline.ran(), ","); got != taskID+"/2,"+taskID+"/3" {
		t.Fatalf("restart ran %s, want items 2 and 3 only", got)
	}
	if got := f.reportEntries(h, user, taskID); strings.Join(got, ",") != "a.zip/report.docx,c.zip/report.docx" {
		t.Fatalf("report entries = %v", got)
	}
	task, err := reports.Get(ctx, f.db, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if it := task.Items[1]; it.Status != reports.StatusFailed || it.Reason != "模拟失败" {
		t.Fatalf("failed item = %+v, want its reason kept", it)
	}
}

func TestATaskWhoseItemsAllFailIsFailed(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	taskID := f.generate(h, user, upload{"fail.zip", "mysql", "1.2.0"})
	if s := f.waitFinished(h, user, taskID); s.Status != "failed" {
		t.Fatalf("status = %+v", s)
	}
	expectAPIError(t, get(h, "/api/reports/download/"+taskID, user), http.StatusConflict, "invalid", "")
}

func TestDownloadRefusesATaskStillGenerating(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"a.zip", "mysql", "1.2.0"})
	expectAPIError(t, get(f.handler, "/api/reports/download/"+taskID, user), http.StatusConflict, "invalid", "尚未生成完成")
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

func TestReportTasksAreHiddenFromOtherEngineersButNotFromAdmins(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user, other, admin := f.token("user"), f.token("other"), f.token("admin")
	taskID := f.generate(h, user, upload{"mall.zip", "mysql", "1.2.0"})
	f.waitFinished(h, user, taskID)
	srv := httptest.NewServer(h)
	defer srv.Close()

	for _, path := range []string{"/api/reports/status/", "/api/reports/download/"} {
		expectAPIError(t, get(h, path+taskID, other), http.StatusNotFound, "not_found", "报告任务不存在")
		if rec := get(h, path+taskID, admin); rec.Code != http.StatusOK {
			t.Fatalf("admin GET %s: %d %s", path, rec.Code, rec.Body)
		}
	}
	if _, resp, err := dialWS(t, srv, taskID, other); err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("other engineer's WebSocket: resp=%v err=%v, want 404", resp, err)
	}
	conn, _, err := dialWS(t, srv, taskID, admin)
	if err != nil {
		t.Fatalf("admin's WebSocket: %v", err)
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

func TestReportRoutesRefuseTheRetiredSharedToken(t *testing.T) {
	f := newReportsFixture(t)
	user := f.token("user")
	taskID := f.generate(f.handler, user, upload{"mall.zip", "mysql", "1.2.0"})
	srv := httptest.NewServer(f.handler)
	defer srv.Close()

	const shared = "ATI" // the retired DBCHECK_API_TOKEN default
	rec := serve(f.handler, generateRequest(t, shared, zipsOf(t, upload{"mall.zip", "mysql", "1.2.0"})))
	expectAPIError(t, rec, http.StatusUnauthorized, "unauthorized", "")
	for _, path := range []string{"/api/reports/status/", "/api/reports/download/"} {
		expectAPIError(t, get(f.handler, path+taskID, shared), http.StatusUnauthorized, "unauthorized", "")
	}
	if _, resp, err := dialWS(t, srv, taskID, shared); err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("WebSocket with the shared token: resp=%v err=%v, want 401", resp, err)
	}
}

func TestWebSocketReplaysProgressAndEndsWithDone(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&fakePipeline{})
	user := f.token("user")
	taskID := f.generate(h, user, upload{"a.zip", "mysql", "1.2.0"}, upload{"b.zip", "mysql", "1.2.0"})
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, _, err := dialWS(t, srv, taskID, user)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if conn.Subprotocol() != user {
		t.Fatalf("subprotocol = %q, want the session token", conn.Subprotocol())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read before done: %v", err)
		}
		if strings.Contains(string(msg), `"type":"done"`) {
			if !strings.Contains(string(msg), "/api/reports/download/"+taskID) {
				t.Fatalf("done = %s", msg)
			}
			return
		}
	}
}

func TestGenerateHandsEveryWDRFileOfAnItemToThePipeline(t *testing.T) {
	f := newReportsFixture(t)
	pipeline := &fakePipeline{}
	h := f.start(pipeline)
	user := f.token("user")
	files := append(zipsOf(t, upload{"gauss.zip", "gaussdb", "1.2.0"}),
		formFile{"wdr_1", "wdr-cluster.html", []byte("<html>")},
		formFile{"wdr_1", "wdr-node.html", []byte("<html>")})
	rec := serve(h, generateRequest(t, user, files))
	if rec.Code != http.StatusOK {
		t.Fatalf("generate: %d %s", rec.Code, rec.Body)
	}
	var body struct {
		TaskID string `json:"task_id"`
	}
	decode(t, rec, &body)
	f.waitFinished(h, user, body.TaskID)

	job := pipeline.jobs[0]
	if len(job.Input.WDRPaths) != 2 || job.Input.WDRPaths[0] == job.Input.WDRPaths[1] || job.Input.AWRPath != "" {
		t.Fatalf("pipeline got %+v", job.Input)
	}
}

func TestGenerateRefusesTwoAWRFilesForOneItem(t *testing.T) {
	f := newReportsFixture(t)
	files := append(zipsOf(t, upload{"ora.zip", "oracle", "1.2.0"}),
		formFile{"awr_1", "awr-a.html", []byte("<html>")},
		formFile{"awr_1", "awr-b.html", []byte("<html>")})
	expectAPIError(t, serve(f.handler, generateRequest(t, f.token("user"), files)), http.StatusBadRequest, "invalid", "AWR")
}

func TestGenerateEnforcesTheUploadLimit(t *testing.T) {
	f := newReportsFixture(t)
	f.cfg.MaxUploadBytes = 64
	h := f.start(&fakePipeline{})
	rec := serve(h, generateRequest(t, f.token("user"), zipsOf(t, upload{"big.zip", "mysql", "1.2.0"})))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body %s)", rec.Code, rec.Body)
	}
}
