package web

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"sync"
)

type LogStream string

const (
	LogStdout LogStream = "stdout"
	LogStderr LogStream = "stderr"
)

type LogEvent struct {
	Stream LogStream
	Line   string
}

type CommandRunner interface {
	Run(command string, args []string, onLog func(LogEvent)) error
}

type ExecRunner struct {
	newCmd func(name string, args ...string) *exec.Cmd
}

func NewExecRunner() *ExecRunner {
	return &ExecRunner{newCmd: exec.Command}
}

func (r *ExecRunner) Run(command string, args []string, onLog func(LogEvent)) error {
	cmd := r.newCmd(command, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe failed: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe failed: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start command failed: %w", err)
	}

	events := make(chan LogEvent, 256)

	var dispatchWg sync.WaitGroup
	dispatchWg.Add(1)
	go func() {
		defer dispatchWg.Done()
		if onLog == nil {
			for range events {
			}
			return
		}
		for ev := range events {
			onLog(ev)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		streamLines(stderr, LogStderr, events)
	}()
	go func() {
		defer wg.Done()
		streamLines(stdout, LogStdout, events)
	}()

	waitErr := cmd.Wait()
	wg.Wait()
	close(events)
	dispatchWg.Wait()
	return waitErr
}

func streamLines(r io.Reader, stream LogStream, out chan<- LogEvent) {
	if out == nil {
		io.Copy(io.Discard, r)
		return
	}
	scanner := bufio.NewScanner(r)
	// Allow up to 1MB log lines.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		out <- LogEvent{Stream: stream, Line: scanner.Text()}
	}
}
