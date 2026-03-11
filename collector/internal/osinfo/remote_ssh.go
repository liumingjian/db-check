package osinfo

import (
	"bytes"
	"context"
	"dbcheck/collector/internal/cli"
	"fmt"
	"os/exec"
	"strings"
)

type remoteRunner interface {
	Run(command string) (string, error)
	Close() error
}

type execRunner struct {
	ctx     context.Context
	cfg     cli.Config
	sshPath string
}

func newSSHRunner(cfg cli.Config) (remoteRunner, error) {
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		return nil, fmt.Errorf("ssh binary not found: %w", err)
	}
	return &execRunner{ctx: context.Background(), cfg: cfg, sshPath: sshPath}, nil
}

func (r *execRunner) Run(command string) (string, error) {
	args := append(r.baseSSHArgs(), "sh", "-lc", shellQuote(command))
	cmd, err := r.buildCommand(args)
	if err != nil {
		return "", err
	}
	return runCommand(cmd)
}

func (r *execRunner) Close() error {
	return nil
}

func (r *execRunner) buildCommand(args []string) (*exec.Cmd, error) {
	if strings.TrimSpace(r.cfg.OSSSHKeyPath) != "" {
		return exec.CommandContext(r.ctx, r.sshPath, args...), nil
	}
	if strings.TrimSpace(r.cfg.OSPassword) == "" {
		return nil, fmt.Errorf("missing remote OS authentication, provide --os-password or --os-ssh-key-path")
	}
	sshpassPath, err := exec.LookPath("sshpass")
	if err != nil {
		return nil, fmt.Errorf("sshpass binary not found for password authentication: %w", err)
	}
	cmd := exec.CommandContext(r.ctx, sshpassPath, append([]string{"-e", r.sshPath}, args...)...)
	cmd.Env = append(cmd.Environ(), "SSHPASS="+r.cfg.OSPassword)
	return cmd, nil
}

func (r *execRunner) baseSSHArgs() []string {
	args := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-p", fmt.Sprintf("%d", r.cfg.OSPort),
	}
	if strings.TrimSpace(r.cfg.OSSSHKeyPath) != "" {
		args = append(args, "-i", r.cfg.OSSSHKeyPath)
	}
	args = append(args, fmt.Sprintf("%s@%s", r.cfg.OSUsername, r.cfg.OSHost))
	return args
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func runCommand(cmd *exec.Cmd) (string, error) {
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("remote command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return string(output), nil
}
