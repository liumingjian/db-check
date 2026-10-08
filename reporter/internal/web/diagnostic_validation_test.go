package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type checkedDiagnosticPipeline struct{ fakePipeline }

func (*checkedDiagnosticPipeline) ValidateDiagnostics(_ context.Context, _ string, input ItemInput) ([]DiagnosticCheck, error) {
	checks := []DiagnosticCheck{}
	if input.AWRPath != "" {
		checks = append(checks, DiagnosticCheck{Kind: "checked", Evidence: "unconfirmed"})
	}
	for range input.WDRPaths {
		checks = append(checks, DiagnosticCheck{Kind: "checked", Evidence: "unconfirmed"})
	}
	return checks, nil
}

func TestWrongDiagnosticFieldDoesNotMislabelSupportedAttachment(t *testing.T) {
	f := newReportsFixture(t)
	h := f.start(&checkedDiagnosticPipeline{})
	files := append(zipsOf(t, upload{"oracle.zip", "oracle", ""}), formFile{"awr_1", "right.html", []byte("contract fixture")}, formFile{"wdr_1", "wrong.html", []byte("contract fixture")})
	req := generateRequest(t, f.token("user"), files)
	req.URL.Path = "/api/reports/validate"
	rec := serve(h, req)
	var checks []DiagnosticCheck
	if err := json.Unmarshal(rec.Body.Bytes(), &checks); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || len(checks) != 2 || checks[0].Kind != "checked" || checks[1].Kind != "invalid" {
		t.Fatalf("validation mislabeled supported attachment: %d %s", rec.Code, rec.Body)
	}
}

func TestDiagnosticValidationDoesNotAdmitTasksAndScopesWrongType(t *testing.T) {
	f := newReportsFixture(t)
	h := f.api.handler()
	files := append(zipsOf(t, upload{"my.zip", "mysql", ""}, upload{"ora.zip", "oracle", ""}), formFile{"wdr_2", "wrong.html", []byte("<html>")})
	req := generateRequest(t, f.token("user"), files)
	req.URL.Path = "/api/reports/validate"
	rec := serve(h, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"itemPosition":2`) || !strings.Contains(rec.Body.String(), `"fileName":"wrong.html"`) || !strings.Contains(rec.Body.String(), `"kind":"invalid"`) {
		t.Fatalf("validation: %d %s", rec.Code, rec.Body)
	}
	rec = get(h, "/api/reports/mine", f.token("user"))
	if rec.Body.String() != "[]\n" {
		t.Fatalf("validation admitted a task: %s", rec.Body)
	}
}

func TestRestartRemovesAbandonedDiagnosticValidationDataOnly(t *testing.T) {
	f := newReportsFixture(t)
	abandoned := filepath.Join(f.cfg.DataDir, "diagnostic-validation-interrupted")
	for _, name := range []string{"uploads/zip-1-customer.zip", "uploads/awr-1-customer.html", "items/1/extract/run/result.json"} {
		path := filepath.Join(abandoned, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("customer data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	preserved := filepath.Join(f.cfg.DataDir, "tasks", "preserved", "uploads", "customer.zip")
	if err := os.MkdirAll(filepath.Dir(preserved), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(preserved, []byte("task input"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "customer.zip")
	if err := os.WriteFile(outsideFile, []byte("outside input"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(f.cfg.DataDir, "diagnostic-validation-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := newAPIHandler(f.cfg, f.platform, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned customer data remains: %v", err)
	}
	for _, path := range []string{preserved, outsideFile} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("unrelated data removed: %s: %v", path, err)
		}
	}
}

func TestDiagnosticValidationUsesTheInjectedRunnerAndRejectsFailures(t *testing.T) {
	for _, tt := range []struct {
		name, output, want string
		runErr             error
	}{
		{name: "process failure", runErr: errors.New("validator unavailable"), want: "validator unavailable"},
		{name: "malformed response", output: "not json", want: "invalid diagnostic validation response"},
		{name: "unsupported result", output: `[{"kind":"certain"}]`, want: "unknown diagnostic validation result"},
		{name: "missing detail", output: `[{"kind":"invalid"}]`, want: "missing diagnostic failure detail"},
		{name: "checked", output: `[{"kind":"checked","evidence":"database_name_dbid"}]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPipeline("fixture-api", "fixture-python")
			p.LayoutResolver = fakeLayoutResolver{}
			p.ExtractZip = func(string, string) error { return nil }
			p.DetectRun = func(string) (string, error) { return "fixture-run", nil }
			runner := &fakeRunner{output: []byte(tt.output), outputErr: tt.runErr}
			p.Runner = runner
			checks, err := p.ValidateDiagnostics(context.Background(), t.TempDir(), ItemInput{ID: "1", AWRPath: "awr.html"})
			if tt.want != "" {
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("error = %v, want %s", err, tt.want)
				}
				return
			}
			if err != nil || len(checks) != 1 || checks[0].Evidence != "database_name_dbid" {
				t.Fatalf("checks = %+v, error = %v", checks, err)
			}
			if !hasArg(runner.lastArgs, "--awr-file", "awr.html") || runner.lastArgs[len(runner.lastArgs)-1] != "--validate-diagnostics" {
				t.Fatalf("validation args = %v", runner.lastArgs)
			}
		})
	}
}
