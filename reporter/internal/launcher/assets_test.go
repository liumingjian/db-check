package launcher

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveAssetLayoutPrefersReleaseLayout(t *testing.T) {
	root := t.TempDir()
	exeDir := filepath.Join(root, "bundle")
	mkdirFile(t, filepath.Join(exeDir, "runtime", "reporter_orchestrator.py"))
	mkdirFile(t, filepath.Join(exeDir, "runtime", "requirements.txt"))
	mkdirFile(t, filepath.Join(exeDir, "assets", "rules", "mysql", "rule.json"))
	mkdirFile(t, filepath.Join(exeDir, "assets", "templates", "mysql-template.docx"))
	layout, err := ResolveAssetLayout(filepath.Join(exeDir, "db-reporter"), Config{})
	if err != nil {
		t.Fatalf("ResolveAssetLayout failed: %v", err)
	}
	if layout.Script != filepath.Join(exeDir, "runtime", "reporter_orchestrator.py") {
		t.Fatalf("unexpected script path: %s", layout.Script)
	}
}

func TestResolveAssetLayoutSupportsRepoLayout(t *testing.T) {
	root := t.TempDir()
	exePath := filepath.Join(root, "bin", "db-reporter")
	mkdirFile(t, filepath.Join(root, "reporter", "cli", "reporter_orchestrator.py"))
	mkdirFile(t, filepath.Join(root, "requirements.txt"))
	mkdirFile(t, filepath.Join(root, "rules", "mysql", "rule.json"))
	mkdirFile(t, filepath.Join(root, "reporter", "templates", "mysql-template.docx"))
	layout, err := ResolveAssetLayout(exePath, Config{})
	if err != nil {
		t.Fatalf("ResolveAssetLayout failed: %v", err)
	}
	if layout.RuleFile != filepath.Join(root, "rules", "mysql", "rule.json") {
		t.Fatalf("unexpected rule path: %s", layout.RuleFile)
	}
}

func mkdirFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(path, []byte("ok"), 0o644); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}
