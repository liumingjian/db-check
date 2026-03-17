package web

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestExecRunnerStreamsStdoutAndStderr(t *testing.T) {
	runner := NewExecRunner()
	runner.newCmd = func(name string, args ...string) *exec.Cmd {
		cmd := exec.Command(name, args...)
		cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
		return cmd
	}

	var events []LogEvent
	err := runner.Run(os.Args[0], []string{"-test.run=TestHelperProcessForExecRunner"}, func(ev LogEvent) {
		events = append(events, ev)
	})
	if err != nil {
		t.Fatalf("runner.Run failed: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least 2 log events, got %d", len(events))
	}
}

func TestHelperProcessForExecRunner(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	fmt.Fprintln(os.Stdout, "stdout line")
	fmt.Fprintln(os.Stderr, "stderr line")
	os.Exit(0)
}
