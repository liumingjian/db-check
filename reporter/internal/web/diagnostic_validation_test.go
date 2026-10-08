package web

import (
	"context"
	"encoding/json"
	"net/http"
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
