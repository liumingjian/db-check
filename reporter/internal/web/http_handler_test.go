package web

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCORSPreflightAllowsConfiguredOrigin(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	handler := h.handler()

	req := httptest.NewRequest(http.MethodOptions, "http://example.com/api/reports/generate", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d got %d", http.StatusNoContent, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://example.com" {
		t.Fatalf("unexpected allow origin header: %q", got)
	}
}

func TestCORSPreflightAllowsWildcardOrigin(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"*"},
		APIToken:       defaultAPIToken,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	handler := h.handler()

	req := httptest.NewRequest(http.MethodOptions, "http://example.com/api/reports/generate", nil)
	req.Header.Set("Origin", "http://evil.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d got %d", http.StatusNoContent, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://evil.com" {
		t.Fatalf("unexpected allow origin header: %q", got)
	}
}

func TestCORSPreflightAllowsTrailingSlashInConfig(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com/"},
		APIToken:       defaultAPIToken,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	handler := h.handler()

	req := httptest.NewRequest(http.MethodOptions, "http://example.com/api/reports/generate", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d got %d", http.StatusNoContent, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://example.com" {
		t.Fatalf("unexpected allow origin header: %q", got)
	}
}

func TestCORSPreflightAllowsHostOnlyEntry(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"localhost:3000"},
		APIToken:       defaultAPIToken,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	handler := h.handler()

	req := httptest.NewRequest(http.MethodOptions, "http://example.com/api/reports/generate", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d got %d", http.StatusNoContent, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Fatalf("unexpected allow origin header: %q", got)
	}
}

func TestCORSPreflightAllowsLocalhostAlias(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://localhost:3000"},
		APIToken:       defaultAPIToken,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	handler := h.handler()

	req := httptest.NewRequest(http.MethodOptions, "http://example.com/api/reports/generate", nil)
	req.Header.Set("Origin", "http://127.0.0.1:3000")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected %d got %d", http.StatusNoContent, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://127.0.0.1:3000" {
		t.Fatalf("unexpected allow origin header: %q", got)
	}
}

func TestCORSRejectsUnknownOrigin(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	handler := h.handler()

	req := httptest.NewRequest(http.MethodGet, "http://evil.com/api/reports/status/any", nil)
	req.Header.Set("Origin", "http://evil.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected %d got %d", http.StatusForbidden, rec.Code)
	}
}

func TestAuthIsRequired(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
	handler := h.handler()

	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/reports/status/any", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected %d got %d", http.StatusUnauthorized, rec.Code)
	}
}

func TestGenerateCreatesTaskRecord(t *testing.T) {
	dataDir := t.TempDir()
	cfg := Config{
		DataDir:        dataDir,
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
		MaxUploadBytes: 0,
		PythonBin:      "python3",
	}
	h := newStartedTestAPIHandler(t, cfg, nil)
	handler := h.handler()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("zips", "demo.zip")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	if _, err := part.Write([]byte("not-a-real-zip")); err != nil {
		t.Fatalf("write part failed: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://example.com/api/reports/generate", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+defaultAPIToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d got %d body=%s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var resp struct {
		TaskID string `json:"task_id"`
		Status string `json:"status"`
		Total  int    `json:"total"`
		WsURL  string `json:"ws_url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.TaskID == "" || resp.Status != "queued" || resp.Total != 1 || resp.WsURL == "" {
		t.Fatalf("unexpected resp: %#v", resp)
	}

	taskPath := filepath.Join(dataDir, "tasks", resp.TaskID, "task.json")
	if _, err := os.Stat(taskPath); err != nil {
		t.Fatalf("expected task.json to exist: %v", err)
	}
}

func TestGenerateAcceptsMultipleIndexedWDRUploads(t *testing.T) {
	dataDir := t.TempDir()
	cfg := Config{
		DataDir:        dataDir,
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
		MaxUploadBytes: 0,
		PythonBin:      "python3",
	}
	h := newStartedTestAPIHandler(t, cfg, nil)

	req := multipartRequestPairs(t, []filePart{
		{field: "zips", name: "demo.zip"},
		{field: "wdr_1", name: "wdr-cluster.html"},
		{field: "wdr_1", name: "wdr-node.html"},
	})
	rec := httptest.NewRecorder()
	h.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d got %d body=%s", http.StatusOK, rec.Code, rec.Body.String())
	}

	ids, err := h.lifecycle.store.ListIDs()
	if err != nil || len(ids) != 1 {
		t.Fatalf("expected one published task, ids=%#v err=%v", ids, err)
	}
	task, err := h.lifecycle.store.Load(ids[0])
	if err != nil {
		t.Fatalf("Load task failed: %v", err)
	}
	items, err := loadTaskInputs(h.lifecycle.store.taskDir(task.ID), task)
	if err != nil {
		t.Fatalf("load task inputs failed: %v", err)
	}
	if len(items) != 1 || len(items[0].WDRPaths) != 2 {
		t.Fatalf("expected two WDRPaths to be staged: %#v", items)
	}
	if items[0].WDRPaths[0] == items[0].WDRPaths[1] {
		t.Fatalf("expected unique WDR upload paths: %#v", items[0].WDRPaths)
	}
	if items[0].AWRPath != "" {
		t.Fatalf("did not expect AWRPath: %#v", items[0])
	}
}

func TestGenerateAcceptsBulkWDRUpload(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
		MaxUploadBytes: 0,
		PythonBin:      "python3",
	}
	h := newStartedTestAPIHandler(t, cfg, nil)

	req := multipartRequest(t, map[string]string{
		"zips": "demo.zip",
		"wdrs": "wdr.html",
	})
	rec := httptest.NewRecorder()
	h.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected %d got %d body=%s", http.StatusOK, rec.Code, rec.Body.String())
	}
}

func TestGenerateRejectsMultipleIndexedAWRUploads(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
		MaxUploadBytes: 0,
		PythonBin:      "python3",
	}
	h := newStartedTestAPIHandler(t, cfg, nil)

	req := multipartRequestPairs(t, []filePart{
		{field: "zips", name: "demo.zip"},
		{field: "awr_1", name: "awr-a.html"},
		{field: "awr_1", name: "awr-b.html"},
	})
	rec := httptest.NewRecorder()
	h.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d got %d body=%s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
}

func TestGenerateEnforcesUploadLimit(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       defaultAPIToken,
		MaxUploadBytes: 64, // tiny
	}
	h := newStartedTestAPIHandler(t, cfg, nil)
	handler := h.handler()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("zips", "demo.zip")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	part.Write(bytes.Repeat([]byte("x"), 1024))
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "http://example.com/api/reports/generate", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+defaultAPIToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected %d got %d body=%s", http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	}
}

func TestGenerateRejectsFullCapacityBeforeReadingMultipartBody(t *testing.T) {
	cfg := Config{
		DataDir:          t.TempDir(),
		AllowedOrigins:   []string{"http://example.com"},
		APIToken:         defaultAPIToken,
		MaxUploadBytes:   0,
		MaxAcceptedTasks: 1,
		PythonBin:        "python3",
	}
	blocked := make(chan struct{})
	h := newStartedTestAPIHandler(t, cfg, func() (*Pipeline, error) {
		pipeline := controlledLifecyclePipeline()
		pipeline.ExtractZip = func(string, string) error {
			<-blocked
			return nil
		}
		return pipeline, nil
	})
	t.Cleanup(func() { close(blocked) })
	handler := h.handler()

	first := multipartRequest(t, map[string]string{"zips": "first.zip"})
	firstRec := httptest.NewRecorder()
	handler.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first submission status=%d body=%s", firstRec.Code, firstRec.Body.String())
	}

	body := &unreadMultipartBody{}
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api/reports/generate", body)
	req.Header.Set("Authorization", "Bearer "+defaultAPIToken)
	req.Header.Set("Content-Type", "multipart/form-data; boundary=never-read")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d got %d body=%s", http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	}
	if body.reads != 0 {
		t.Fatalf("full-capacity request read multipart body %d times", body.reads)
	}
	var response struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode busy response: %v", err)
	}
	if response.Code != "capacity_exhausted" || response.Error != ErrTaskCapacity.Error() {
		t.Fatalf("unexpected busy response: %#v", response)
	}
}

type unreadMultipartBody struct {
	reads int
}

func newStartedTestAPIHandler(t *testing.T, cfg Config, factory pipelineFactory) *apiHandler {
	t.Helper()
	if factory == nil {
		factory = controlledRecoveryPipeline
	}
	lifecycle, err := newTaskLifecycle(cfg, factory)
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
	return newAPIHandlerWithLifecycle(cfg, lifecycle)
}

func (b *unreadMultipartBody) Read([]byte) (int, error) {
	b.reads++
	return 0, io.EOF
}

func (b *unreadMultipartBody) Close() error {
	return nil
}

func multipartRequest(t *testing.T, files map[string]string) *http.Request {
	t.Helper()
	parts := make([]filePart, 0, len(files))
	for field, name := range files {
		parts = append(parts, filePart{field: field, name: name})
	}
	return multipartRequestPairs(t, parts)
}

type filePart struct {
	field string
	name  string
}

func multipartRequestPairs(t *testing.T, files []filePart) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, file := range files {
		part, err := mw.CreateFormFile(file.field, file.name)
		if err != nil {
			t.Fatalf("CreateFormFile failed: %v", err)
		}
		if _, err := part.Write([]byte("content")); err != nil {
			t.Fatalf("write part failed: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart failed: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "http://example.com/api/reports/generate", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+defaultAPIToken)
	return req
}
