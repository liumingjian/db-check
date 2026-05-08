package web

import (
	"strings"
	"testing"
)

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
