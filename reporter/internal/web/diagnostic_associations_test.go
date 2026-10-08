package web

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
)

func TestGeneratePreservesIndexedDiagnosticsInMixedBatch(t *testing.T) {
	f := newReportsFixture(t)
	pipeline := &fakePipeline{}
	h := f.start(pipeline)
	user := f.token("user")
	files := append(zipsOf(t, upload{"my.zip", "mysql", "1.2.0"}, upload{"ora.zip", "oracle", "1.2.0"}, upload{"gs.zip", "gaussdb", "1.2.0"}),
		formFile{"awr_2", "awr.html", []byte("oracle attachment")},
		formFile{"wdr_3", "wdr-one.html", []byte("first gauss attachment")},
		formFile{"wdr_3", "wdr-two.htm", []byte("second gauss attachment")})
	rec := serve(h, generateRequest(t, user, files))
	if rec.Code != http.StatusOK {
		t.Fatalf("generate: %d %s", rec.Code, rec.Body)
	}
	var body struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	f.waitFinished(h, user, body.TaskID)
	for _, job := range pipeline.jobs {
		assertDiagnosticAssociation(t, job.Input)
	}
}

func assertDiagnosticAssociation(t *testing.T, input ItemInput) {
	t.Helper()
	switch input.Name {
	case "my.zip":
		if input.AWRPath != "" || len(input.WDRPaths) != 0 {
			t.Fatalf("MySQL got diagnostics: %+v", input)
		}
	case "ora.zip":
		if len(input.WDRPaths) != 0 {
			t.Fatalf("Oracle got WDRs: %+v", input)
		}
		assertDiagnosticContent(t, input.AWRPath, "oracle attachment")
	case "gs.zip":
		if input.AWRPath != "" || len(input.WDRPaths) != 2 {
			t.Fatalf("GaussDB got wrong diagnostics: %+v", input)
		}
		assertDiagnosticContent(t, input.WDRPaths[0], "first gauss attachment")
		assertDiagnosticContent(t, input.WDRPaths[1], "second gauss attachment")
	default:
		t.Fatalf("unexpected item: %+v", input)
	}
}

func assertDiagnosticContent(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("attachment %s = %q, want %q", path, data, want)
	}
}
