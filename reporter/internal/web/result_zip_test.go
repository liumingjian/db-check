package web

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildResultZipPublishesValidatedArchive(t *testing.T) {
	dir := t.TempDir()
	report := filepath.Join(dir, "report.docx")
	if err := writeTestReportDocx(report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	zipPath := filepath.Join(dir, "reports.zip")

	if err := buildResultZip(zipPath, []ItemResult{{ID: "1", Status: ItemDone, ReportDocx: report}}, []ItemInput{{ID: "1", Name: "oracle.zip"}}); err != nil {
		t.Fatalf("build result zip: %v", err)
	}
	if err := validateResultZip(zipPath); err != nil {
		t.Fatalf("published result zip is invalid: %v", err)
	}
	if _, err := os.Stat(zipPath + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary result zip remains after publication: %v", err)
	}
}

func TestBuildResultZipDoesNotPublishWhenArtifactReadFails(t *testing.T) {
	dir := t.TempDir()
	report := filepath.Join(dir, "report.docx")
	if err := writeTestReportDocx(report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	zipPath := filepath.Join(dir, "reports.zip")
	readErr := errors.New("injected artifact read failure")

	err := buildResultZipWithDependencies(zipPath, []ItemResult{{ID: "1", Status: ItemDone, ReportDocx: report}}, []ItemInput{{ID: "1", Name: "oracle.zip"}}, resultZipDependencies{
		openFile: func(string) (io.ReadCloser, error) {
			return nil, readErr
		},
		validate: validateResultZip,
	})
	if !errors.Is(err, readErr) {
		t.Fatalf("build result zip error=%v, want injected read failure", err)
	}
	if _, err := os.Stat(zipPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read failure published result zip: %v", err)
	}
}

func TestBuildResultZipDoesNotPublishBeforeTemporaryValidation(t *testing.T) {
	dir := t.TempDir()
	report := filepath.Join(dir, "report.docx")
	if err := writeTestReportDocx(report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	zipPath := filepath.Join(dir, "reports.zip")
	validationErr := errors.New("injected archive validation failure")
	validated := false

	err := buildResultZipWithDependencies(zipPath, []ItemResult{{ID: "1", Status: ItemDone, ReportDocx: report}}, []ItemInput{{ID: "1", Name: "oracle.zip"}}, resultZipDependencies{
		openFile: func(path string) (io.ReadCloser, error) {
			return os.Open(path)
		},
		validate: func(path string) error {
			validated = true
			if path != zipPath+".tmp" {
				t.Fatalf("validated path=%q, want temporary archive", path)
			}
			if err := validateResultZip(path); err != nil {
				t.Fatalf("temporary archive was not valid after close: %v", err)
			}
			return validationErr
		},
	})
	if !validated || !errors.Is(err, validationErr) {
		t.Fatalf("build result zip error=%v, validated=%v", err, validated)
	}
	if _, err := os.Stat(zipPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation failure published result zip: %v", err)
	}
	if _, err := os.Stat(zipPath + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation failure left temporary result zip: %v", err)
	}
}

func TestBuildResultZipReportsFailedItemsWhenNoSuccess(t *testing.T) {
	err := buildResultZip(
		t.TempDir()+"/reports.zip",
		[]ItemResult{
			{
				ID:     "1",
				Status: ItemFailed,
				Error:  "orchestrator failed: missing module docx",
			},
		},
		[]ItemInput{{ID: "1", Name: "oracle.zip"}},
	)
	if err == nil {
		t.Fatalf("expected error")
	}
	got := err.Error()
	for _, want := range []string{
		"no successful reports to download",
		"oracle.zip",
		"orchestrator failed: missing module docx",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected %q to contain %q", got, want)
		}
	}
}

func TestBuildResultZipRejectsDoneItemWithoutReportArtifact(t *testing.T) {
	err := buildResultZip(
		t.TempDir()+"/reports.zip",
		[]ItemResult{{ID: "1", Status: ItemDone}},
		[]ItemInput{{ID: "1", Name: "oracle.zip"}},
	)
	if err == nil || !strings.Contains(err.Error(), "completed report artifact is missing") {
		t.Fatalf("result zip error=%v, want missing completed artifact", err)
	}
}
