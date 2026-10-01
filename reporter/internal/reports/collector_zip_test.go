package reports

import (
	"archive/zip"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeZip writes a ZIP whose entries are path -> content; a string content
// is written as-is, anything else JSON-encoded.
func writeZip(t *testing.T, entries map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.zip")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	for name, content := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		raw, ok := content.(string)
		if !ok {
			b, err := json.Marshal(content)
			if err != nil {
				t.Fatal(err)
			}
			raw = string(b)
		}
		if _, err := w.Write([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeRaw(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.zip")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func manifest(dbType string, result ...string) map[string]any {
	resultName := "result.json"
	if len(result) > 0 {
		resultName = result[0]
	}
	return map[string]any{"schema_version": "1.0", "db_type": dbType, "artifacts": map[string]any{"result": resultName}}
}

func result(version any) map[string]any {
	return map[string]any{"meta": map[string]any{"schema_version": "2.0", "collector_version": version}}
}

func ptr(s string) *string { return &s }

// The cases mirror the browser's inspection (web/src/lib/report-input/inspect.test.ts);
// the server's reading differs only where decisions.md item 3 keeps the
// launcher's db_type normalisation.
func TestReadCollectorZipFindsDBTypeAndCollectorVersion(t *testing.T) {
	cases := []struct {
		name        string
		entries     map[string]any
		wantDBType  string
		wantVersion *string
	}{
		{"mysql at the root", map[string]any{"manifest.json": manifest("mysql"), "result.json": result("1.2.0")}, "mysql", ptr("1.2.0")},
		{"oracle run dir in a subdirectory, custom result name", map[string]any{
			"oracle-10.0.0.8-20260312/manifest.json":     manifest("oracle", "result_oracle.json"),
			"oracle-10.0.0.8-20260312/result_oracle.json": result("1.1.0"),
		}, "oracle", ptr("1.1.0")},
		{"run dir two levels down", map[string]any{"a/b/manifest.json": manifest("mysql"), "a/b/result.json": result("1.2.0")}, "mysql", ptr("1.2.0")},
		{"gaussdb with macOS artifacts", map[string]any{
			"run/manifest.json":          manifest("gaussdb"),
			"run/result.json":            result("1.2.0"),
			"__MACOSX/run/manifest.json": manifest("mysql"),
			"run/._manifest.json":        "junk",
		}, "gaussdb", ptr("1.2.0")},
		{"backslash paths", map[string]any{`run\manifest.json`: manifest("mysql"), `run\result.json`: result("1.2.0")}, "mysql", ptr("1.2.0")},
		{"manifest names no result", map[string]any{"manifest.json": map[string]any{"db_type": "mysql"}}, "mysql", nil},
		{"named result missing", map[string]any{"manifest.json": manifest("oracle", "missing.json")}, "oracle", nil},
		{"result not JSON", map[string]any{"manifest.json": manifest("mysql"), "result.json": "{oops"}, "mysql", nil},
		{"meta has no collector_version", map[string]any{"manifest.json": manifest("mysql"), "result.json": map[string]any{"meta": map[string]any{}}}, "mysql", nil},
		{"blank collector_version", map[string]any{"manifest.json": manifest("mysql"), "result.json": result("  ")}, "mysql", nil},
		{"non-string collector_version", map[string]any{"manifest.json": manifest("mysql"), "result.json": result(12)}, "mysql", nil},
		{"collector_version taken as written, trimmed", map[string]any{"manifest.json": manifest("mysql"), "result.json": result(" 1.3.0-rc1 ")}, "mysql", ptr("1.3.0-rc1")},
		// The launcher's normalisation, kept on the server (decisions.md item 3).
		{"db_type trimmed and lowercased", map[string]any{"manifest.json": manifest(" MySQL "), "result.json": result("1.2.0")}, "mysql", ptr("1.2.0")},
		{"db_type from result meta when the manifest has none", map[string]any{
			"manifest.json": map[string]any{"artifacts": map[string]any{"result": "result.json"}},
			"result.json":   map[string]any{"meta": map[string]any{"db_type": "oracle", "collector_version": "1.2.0"}},
		}, "oracle", ptr("1.2.0")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ReadCollectorZip(writeZip(t, c.entries))
			if err != nil {
				t.Fatalf("ReadCollectorZip: %v", err)
			}
			if got.DBType != c.wantDBType {
				t.Errorf("DBType = %q, want %q", got.DBType, c.wantDBType)
			}
			switch {
			case c.wantVersion == nil && got.CollectorVersion != nil:
				t.Errorf("CollectorVersion = %q, want none", *got.CollectorVersion)
			case c.wantVersion != nil && (got.CollectorVersion == nil || *got.CollectorVersion != *c.wantVersion):
				t.Errorf("CollectorVersion = %v, want %q", got.CollectorVersion, *c.wantVersion)
			}
		})
	}
}

func TestReadCollectorZipRefusesUnusableZips(t *testing.T) {
	cases := []struct {
		name   string
		path   func(t *testing.T) string
		reason string
	}{
		{"corrupt ZIP", func(t *testing.T) string { return writeRaw(t, "PK this is not really a zip") }, "不是有效的 ZIP 文件"},
		{"empty file", func(t *testing.T) string { return writeRaw(t, "") }, "不是有效的 ZIP 文件"},
		{"no manifest", func(t *testing.T) string { return writeZip(t, map[string]any{"result.json": result("1.2.0")}) }, "没有 manifest.json"},
		{"manifest only under __MACOSX", func(t *testing.T) string {
			return writeZip(t, map[string]any{"__MACOSX/manifest.json": manifest("mysql")})
		}, "没有 manifest.json"},
		{"two run dirs", func(t *testing.T) string {
			return writeZip(t, map[string]any{"a/manifest.json": manifest("mysql"), "b/manifest.json": manifest("mysql")})
		}, "2 个 manifest.json"},
		{"manifest not JSON", func(t *testing.T) string { return writeZip(t, map[string]any{"manifest.json": "{ db_type: mysql"}) }, "无法解析"},
		{"manifest is an array", func(t *testing.T) string { return writeZip(t, map[string]any{"manifest.json": "[]"}) }, "无法解析"},
		{"no db_type", func(t *testing.T) string {
			return writeZip(t, map[string]any{"manifest.json": map[string]any{"schema_version": "1.0"}})
		}, "缺少 db_type"},
		{"unsupported db_type", func(t *testing.T) string { return writeZip(t, map[string]any{"manifest.json": manifest("dameng")}) }, "不支持的数据库类型：dameng"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ReadCollectorZip(c.path(t))
			if err == nil || !strings.Contains(err.Error(), c.reason) {
				t.Fatalf("err = %v, want a reason containing %q", err, c.reason)
			}
		})
	}
}
