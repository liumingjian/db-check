package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDefaultAPITokenAuthorizesStatusProbe(t *testing.T) {
	cfg, err := ParseConfig(nil, envGetter(map[string]string{
		envDataDir:        t.TempDir(),
		envAllowedOrigins: "http://example.com",
	}))
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}
	h, err := newAPIHandler(cfg, false)
	if err != nil {
		t.Fatalf("newAPIHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/reports/status/frontend-probe", nil)
	req.Header.Set("Authorization", "Bearer "+defaultAPIToken)
	rec := httptest.NewRecorder()
	h.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected %d got %d body=%s", http.StatusNotFound, rec.Code, rec.Body.String())
	}
}
