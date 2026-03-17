package web

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCORSPreflightAllowsConfiguredOrigin(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       "secret",
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
		APIToken:       "secret",
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
		APIToken:       "secret",
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
		APIToken:       "secret",
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
		APIToken:       "secret",
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
		APIToken:       "secret",
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
		APIToken:       "secret",
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
		APIToken:       "secret",
		MaxUploadBytes: 0,
		PythonBin:      "python3",
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
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
	req.Header.Set("Authorization", "Bearer secret")
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
	if resp.TaskID == "" || resp.Status != "processing" || resp.Total != 1 || resp.WsURL == "" {
		t.Fatalf("unexpected resp: %#v", resp)
	}

	taskPath := filepath.Join(dataDir, "tasks", resp.TaskID, "task.json")
	if _, err := os.Stat(taskPath); err != nil {
		t.Fatalf("expected task.json to exist: %v", err)
	}
}

func TestGenerateEnforcesUploadLimit(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
		APIToken:       "secret",
		MaxUploadBytes: 64, // tiny
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}
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
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected %d got %d body=%s", http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	}
}
