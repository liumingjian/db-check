package cli

import (
	"strings"
	"testing"
)

func TestParseArgsOSOnlyValid(t *testing.T) {
	args := []string{"--db-type", "mysql", "--os-only"}
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

func TestParseArgsRejectsOSSkip(t *testing.T) {
	args := []string{"--db-type", "mysql", "--os-skip"}
	_, err := ParseArgs(args)
	if err == nil {
		t.Fatalf("expected unknown os-skip error")
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

func TestParseArgsAcceptsGaussDBInput(t *testing.T) {
	cfg, err := ParseArgs([]string{"--db-type", "gaussdb", "--db-host", "10.0.0.8", "--db-username", "rdsAdmin", "--db-password", "secret", "--dbname", "postgres"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.DBType != "gaussdb" {
		t.Fatalf("unexpected db type: %s", cfg.DBType)
	}
	if cfg.DBPort != DefaultGaussDBPort {
		t.Fatalf("expected gaussdb default port %d, got %d", DefaultGaussDBPort, cfg.DBPort)
	}
	if cfg.GaussUser != "" {
		t.Fatalf("expected empty gauss user in SQL-first mode, got %s", cfg.GaussUser)
	}
	if cfg.GaussEnvFile != "" {
		t.Fatalf("expected empty gauss env file in SQL-first mode, got %s", cfg.GaussEnvFile)
	}
}

func TestParseArgsAcceptsDeprecatedGaussShellOptions(t *testing.T) {
	cfg, err := ParseArgs([]string{
		"--db-type", "gaussdb",
		"--db-host", "10.0.0.8",
		"--db-username", "rdsAdmin",
		"--db-password", "secret",
		"--dbname", "postgres",
		"--gauss-user", "Ruby",
		"--gauss-env-file", "~/gauss_env_file",
	})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.GaussUser != "Ruby" || cfg.GaussEnvFile != "~/gauss_env_file" {
		t.Fatalf("expected deprecated options to parse for explicit ignore, got %+v", cfg)
	}
}

func TestUsageIncludesRemoteOSParameters(t *testing.T) {
	usage := Usage()
	for _, token := range []string{"--os-host", "--os-port", "--os-username", "--os-password", "--os-ssh-key-path", "--gauss-user", "--gauss-env-file"} {
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

func TestParseArgsMainFlowDoesNotUseRemoteOSByDefault(t *testing.T) {
	cfg, err := ParseArgs([]string{"--db-type", "mysql", "--db-host", "10.0.0.1", "--db-username", "root", "--db-password", "secret", "--dbname", "mysql"})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.UseRemoteOS {
		t.Fatalf("expected UseRemoteOS=false without explicit OS parameters")
	}
}

func TestParseArgsExplicitOSHostEnablesRemoteOS(t *testing.T) {
	cfg, err := ParseArgs([]string{
		"--db-type", "mysql",
		"--db-host", "10.0.0.1",
		"--db-username", "root",
		"--db-password", "secret",
		"--dbname", "mysql",
		"--os-host", "10.0.0.2",
	})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if !cfg.UseRemoteOS {
		t.Fatalf("expected UseRemoteOS=true with explicit OS host")
	}
	if cfg.OSUsername != "root" || cfg.OSPassword != "secret" {
		t.Fatalf("unexpected derived OS credentials: %+v", cfg)
	}
}

func TestParseArgsLocalDoesNotUseRemoteOS(t *testing.T) {
	cfg, err := ParseArgs([]string{
		"--db-type", "mysql",
		"--db-host", "127.0.0.1",
		"--db-username", "root",
		"--db-password", "secret",
		"--dbname", "mysql",
		"--local",
	})
	if err != nil {
		t.Fatalf("ParseArgs failed: %v", err)
	}
	if cfg.UseRemoteOS {
		t.Fatalf("expected UseRemoteOS=false for local OS collection")
	}
}

func TestParseArgsRejectsSamplingWithoutOSCollectionTarget(t *testing.T) {
	_, err := ParseArgs([]string{
		"--db-type", "mysql",
		"--db-host", "10.0.0.1",
		"--db-username", "root",
		"--db-password", "secret",
		"--dbname", "mysql",
		"--os-collect-interval", "5",
		"--os-collect-count", "2",
	})
	if err == nil {
		t.Fatalf("expected sampling without OS target to fail")
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
