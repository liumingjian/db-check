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

func TestResolveAssetLayoutUsesOracleRuleForOracleRunDirInReleaseLayout(t *testing.T) {
	root := t.TempDir()
	exeDir := filepath.Join(root, "bundle")
	runDir := filepath.Join(root, "runs", "oracle-demo")
	mkdirFile(t, filepath.Join(exeDir, "runtime", "reporter_orchestrator.py"))
	mkdirFile(t, filepath.Join(exeDir, "runtime", "requirements.txt"))
	mkdirFile(t, filepath.Join(exeDir, "assets", "rules", "mysql", "rule.json"))
	mkdirFile(t, filepath.Join(exeDir, "assets", "rules", "oracle", "rule.json"))
	mkdirFile(t, filepath.Join(exeDir, "assets", "templates", "mysql-template.docx"))
	mkdirFile(t, filepath.Join(runDir, "manifest.json"))
	if err := os.WriteFile(filepath.Join(runDir, "manifest.json"), []byte(`{"db_type":"oracle"}`), 0o644); err != nil {
		t.Fatalf("write manifest failed: %v", err)
	}
	layout, err := ResolveAssetLayout(filepath.Join(exeDir, "db-reporter"), Config{RunDir: runDir})
	if err != nil {
		t.Fatalf("ResolveAssetLayout failed: %v", err)
	}
	if layout.RuleFile != filepath.Join(exeDir, "assets", "rules", "oracle", "rule.json") {
		t.Fatalf("unexpected release oracle rule path: %s", layout.RuleFile)
	}
}

func TestResolveAssetLayoutUsesOracleRuleForOracleRunDir(t *testing.T) {
	root := t.TempDir()
	exePath := filepath.Join(root, "bin", "db-reporter")
	runDir := filepath.Join(root, "runs", "oracle-demo")
	mkdirFile(t, filepath.Join(root, "reporter", "cli", "reporter_orchestrator.py"))
	mkdirFile(t, filepath.Join(root, "requirements.txt"))
	mkdirFile(t, filepath.Join(root, "rules", "mysql", "rule.json"))
	mkdirFile(t, filepath.Join(root, "rules", "oracle", "rule.json"))
	mkdirFile(t, filepath.Join(root, "reporter", "templates", "mysql-template.docx"))
	mkdirFile(t, filepath.Join(runDir, "manifest.json"))
	if err := os.WriteFile(filepath.Join(runDir, "manifest.json"), []byte(`{"db_type":"oracle"}`), 0o644); err != nil {
		t.Fatalf("write manifest failed: %v", err)
	}
	layout, err := ResolveAssetLayout(exePath, Config{RunDir: runDir})
	if err != nil {
		t.Fatalf("ResolveAssetLayout failed: %v", err)
	}
	if layout.RuleFile != filepath.Join(root, "rules", "oracle", "rule.json") {
		t.Fatalf("unexpected oracle rule path: %s", layout.RuleFile)
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
