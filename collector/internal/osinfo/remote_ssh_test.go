package osinfo

import (
	"os/exec"
	"strings"
	"testing"
)

func TestRunCommandIgnoresStderrOnSuccess(t *testing.T) {
	cmd := exec.Command("sh", "-c", "printf 'Linux'; printf 'Warning\\n' >&2")
	output, err := runCommand(cmd)
	if err != nil {
		t.Fatalf("runCommand returned error: %v", err)
	}
	if output != "Linux" {
		t.Fatalf("unexpected output: %q", output)
	}
}

func TestRunCommandIncludesStderrOnFailure(t *testing.T) {
	cmd := exec.Command("sh", "-c", "printf 'permission denied\\n' >&2; exit 1")
	_, err := runCommand(cmd)
	if err == nil {
		t.Fatalf("expected runCommand to fail")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected stderr in error, got: %v", err)
	}
}
