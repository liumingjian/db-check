package cli

import (
	"strings"
	"testing"
)

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

func TestParseArgsAcceptsOracleInput(t *testing.T) {
	cfg, err := ParseArgs([]string{"--db-type", "oracle", "--db-host", "10.0.0.1", "--db-username", "system", "--db-password", "secret", "--dbname", "ORCL"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.DBType != "oracle" {
		t.Fatalf("unexpected db type: %s", cfg.DBType)
	}
	if cfg.DBPort != DefaultOraclePort {
		t.Fatalf("expected oracle default port %d, got %d", DefaultOraclePort, cfg.DBPort)
	}
}

func TestUsageIncludesRemoteOSParameters(t *testing.T) {
	usage := Usage()
	for _, token := range []string{"--os-host", "--os-port", "--os-username", "--os-password", "--os-ssh-key-path"} {
		if !strings.Contains(usage, token) {
			t.Fatalf("expected usage to contain %s", token)
		}
	}
}

func TestParseArgsAppliesRemoteOSDefaults(t *testing.T) {
	cfg, err := ParseArgs([]string{"--db-type", "mysql", "--db-host", "10.0.0.1", "--db-username", "root", "--db-password", "secret", "--dbname", "mysql"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.OSHost != "10.0.0.1" || cfg.OSUsername != "root" || cfg.OSPassword != "secret" {
		t.Fatalf("unexpected derived SSH defaults: %+v", cfg)
	}
}

func TestParseArgsOSOnlyDefaultsToLocalOSCollection(t *testing.T) {
	cfg, err := ParseArgs([]string{"--db-type", "mysql", "--os-only", "--output-dir", "./runs"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.UseRemoteOS {
		t.Fatalf("expected UseRemoteOS=false for local os-only flow")
	}
}

func TestParseArgsMainFlowUsesRemoteOSByDefault(t *testing.T) {
	cfg, err := ParseArgs([]string{"--db-type", "mysql", "--db-host", "10.0.0.1", "--db-username", "root", "--db-password", "secret", "--dbname", "mysql"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if !cfg.UseRemoteOS {
		t.Fatalf("expected UseRemoteOS=true for remote main flow")
	}
}

func TestParseArgsMySQLDefaultsPortTo3306(t *testing.T) {
	cfg, err := ParseArgs([]string{"--db-type", "mysql", "--db-host", "10.0.0.1", "--db-username", "root", "--db-password", "secret", "--dbname", "mysql"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.DBPort != DefaultMySQLPort {
		t.Fatalf("expected mysql default port %d, got %d", DefaultMySQLPort, cfg.DBPort)
	}
}
