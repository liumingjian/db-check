package cli

import (
	"strings"
	"testing"
)

func TestParseArgsAcceptsOracleServiceName(t *testing.T) {
	cfg, err := ParseArgs([]string{
		"--db-type", "oracle", "--db-host", "10.0.0.1",
		"--db-username", "system", "--db-password", "secret",
		"--oracle-service-name", "ORCLPDB1",
	})
	if err != nil {
		t.Fatalf("service-name connection rejected: %v", err)
	}
	if cfg.OracleServiceName != "ORCLPDB1" || cfg.DBName != "" {
		t.Fatalf("unexpected Oracle target: %+v", cfg)
	}
}

func TestParseArgsAcceptsOracleSYSDBA(t *testing.T) {
	cfg, err := ParseArgs([]string{
		"--db-type", "oracle", "--db-host", "10.0.0.1",
		"--db-username", "inspector", "--db-password", "secret",
		"--dbname", "ORCL", "--oracle-sysdba",
	})
	if err != nil || !cfg.OracleSYSDBA {
		t.Fatalf("SYSDBA flag rejected: cfg=%+v err=%v", cfg, err)
	}
}

func TestParseArgsRejectsAmbiguousOracleTarget(t *testing.T) {
	_, err := ParseArgs([]string{
		"--db-type", "oracle", "--db-host", "10.0.0.1",
		"--db-username", "system", "--db-password", "secret",
		"--dbname", "ORCL", "--oracle-service-name", "ORCLPDB1",
	})
	if err == nil || !strings.Contains(err.Error(), "互斥") {
		t.Fatalf("expected ambiguous-target error, got %v", err)
	}
}

func TestParseArgsRejectsOracleOptionsForOtherDatabases(t *testing.T) {
	for _, option := range [][]string{{"--oracle-service-name", "ORCLPDB1"}, {"--oracle-sysdba"}} {
		args := []string{"--db-type", "mysql", "--db-host", "10.0.0.1", "--db-username", "root", "--db-password", "secret", "--dbname", "mysql"}
		_, err := ParseArgs(append(args, option...))
		if err == nil || !strings.Contains(err.Error(), "仅适用于 Oracle") {
			t.Fatalf("expected Oracle-only error for %v, got %v", option, err)
		}
	}
}

func TestOracleUsageListsConnectionOptions(t *testing.T) {
	for _, option := range []string{"--oracle-service-name", "--oracle-sysdba"} {
		if !strings.Contains(Usage(), option) {
			t.Fatalf("missing connection option %s", option)
		}
	}
}
