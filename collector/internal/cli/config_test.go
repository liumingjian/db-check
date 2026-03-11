package cli

import "testing"

func TestParseArgsOSOnlyValid(t *testing.T) {
	args := []string{"--db-type", "mysql", "--os-only", "--output-dir", "./runs"}
	cfg, err := ParseArgs(args)
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if !cfg.OSOnly {
		t.Fatalf("expected OSOnly=true")
	}
	if cfg.OutputDir != "./runs" {
		t.Fatalf("unexpected output dir: %s", cfg.OutputDir)
	}
}

func TestParseArgsRejectsMutualExclusiveFlags(t *testing.T) {
	args := []string{"--db-type", "mysql", "--os-skip", "--os-only"}
	_, err := ParseArgs(args)
	if err == nil {
		t.Fatalf("expected mutual exclusion error")
	}
}

func TestParseArgsRequiresDBInMainFlow(t *testing.T) {
	args := []string{"--db-type", "mysql", "--db-host", "127.0.0.1"}
	_, err := ParseArgs(args)
	if err == nil {
		t.Fatalf("expected missing db credentials error")
	}
}

func TestParseArgsRejectsOracleInput(t *testing.T) {
	args := []string{"--db-type", "oracle", "--os-only"}
	_, err := ParseArgs(args)
	if err == nil {
		t.Fatalf("expected oracle rejection")
	}
}
