package launcher

import (
	"path/filepath"
	"testing"
)

func TestParseArgsDefaultsOutDocxToRunDir(t *testing.T) {
	cfg, err := ParseArgs([]string{"--run-dir", "./runs/demo"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	expected := filepath.Clean("./runs/demo/report.docx")
	if cfg.OutDocx != expected {
		t.Fatalf("unexpected out-docx: got %s want %s", cfg.OutDocx, expected)
	}
}

func TestParseArgsRequiresRunDir(t *testing.T) {
	_, err := ParseArgs(nil)
	if err == nil {
		t.Fatalf("expected missing run-dir error")
	}
}

func TestParseArgsHelp(t *testing.T) {
	_, err := ParseArgs([]string{"--help"})
	if !IsHelp(err) {
		t.Fatalf("expected help error, got %v", err)
	}
}

func TestParseArgsRejectsAWRFileWithRuleFile(t *testing.T) {
	_, err := ParseArgs([]string{"--run-dir", "./runs/demo", "--awr-file", "./awr.html", "--rule-file", "./rule.json"})
	if err == nil {
		t.Fatalf("expected awr-file + rule-file error")
	}
}

func TestOrchestratorArgsPassesMultipleWDRFiles(t *testing.T) {
	cfg := Config{
		RunDir:   "/tmp/run",
		OutDocx:  "/tmp/report.docx",
		WDRFiles: []string{"/tmp/wdr-cluster.html", "/tmp/wdr-node.html"},
	}
	layout := AssetLayout{RuleFile: "rule.json", TemplateFile: "template.docx"}
	args := OrchestratorArgs(cfg, layout)
	if !containsArgPair(args, "--wdr-file", "/tmp/wdr-cluster.html") {
		t.Fatalf("missing cluster WDR arg: %#v", args)
	}
	if !containsArgPair(args, "--wdr-file", "/tmp/wdr-node.html") {
		t.Fatalf("missing node WDR arg: %#v", args)
	}
}

func containsArgPair(args []string, flag string, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}
