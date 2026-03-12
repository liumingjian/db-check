package osinfo

import (
	"dbcheck/collector/internal/cli"
	"os"
	"strings"
	"testing"
)

func TestBuildSSHAuthMethodRequiresAuth(t *testing.T) {
	_, err := buildSSHAuthMethod(cli.Config{})
	if err == nil {
		t.Fatalf("expected auth error")
	}
	if !strings.Contains(err.Error(), "missing remote OS authentication") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildSSHAuthMethodUsesPassword(t *testing.T) {
	method, err := buildSSHAuthMethod(cli.Config{OSPassword: "secret"})
	if err != nil {
		t.Fatalf("buildSSHAuthMethod failed: %v", err)
	}
	if method == nil {
		t.Fatalf("expected auth method")
	}
}

func TestLoadPrivateKeyRejectsInvalidKey(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "invalid-key")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	if _, err := file.WriteString("not-a-private-key"); err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	_, err = loadPrivateKey(file.Name())
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if !strings.Contains(err.Error(), "parse ssh private key failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeRemoteArch(t *testing.T) {
	value, err := normalizeRemoteArch("x86_64")
	if err != nil {
		t.Fatalf("normalizeRemoteArch failed: %v", err)
	}
	if value != "amd64" {
		t.Fatalf("unexpected arch: %s", value)
	}
}
