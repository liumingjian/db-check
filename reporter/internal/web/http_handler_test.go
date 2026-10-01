package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSPreflightAllowsConfiguredOrigin(t *testing.T) {
	cfg := Config{
		DataDir:        t.TempDir(),
		AllowedOrigins: []string{"http://example.com"},
	}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
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
	}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
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
	}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
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
	}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
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
	}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
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
	}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
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
	}
	h, err := newAPIHandler(cfg, testPlatform(t, cfg.DataDir), false)
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
