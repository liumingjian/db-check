package osinfo

import (
	"dbcheck/collector/internal/cli"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const remoteSSHDialTimeout = 10 * time.Second

type execRunner struct {
	client *ssh.Client
	sftp   *sftp.Client
}

type CommandRunner interface {
	Run(command string) (string, error)
	Close() error
}

func NewSSHCommandRunner(cfg cli.Config) (CommandRunner, error) {
	return newSSHRunner(cfg)
}

func newSSHRunner(cfg cli.Config) (remoteRunner, error) {
	clientConfig, err := newSSHClientConfig(cfg)
	if err != nil {
		return nil, err
	}
	address := fmt.Sprintf("%s:%d", strings.TrimSpace(cfg.OSHost), cfg.OSPort)
	client, err := ssh.Dial("tcp", address, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("ssh dial failed: %w", err)
	}
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("sftp client create failed: %w", err)
	}
	return &execRunner{client: client, sftp: sftpClient}, nil
}

func newSSHClientConfig(cfg cli.Config) (*ssh.ClientConfig, error) {
	authMethod, err := buildSSHAuthMethod(cfg)
	if err != nil {
		return nil, err
	}
	return &ssh.ClientConfig{
		User:            strings.TrimSpace(cfg.OSUsername),
		Auth:            []ssh.AuthMethod{authMethod},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         remoteSSHDialTimeout,
	}, nil
}

func buildSSHAuthMethod(cfg cli.Config) (ssh.AuthMethod, error) {
	keyPath := strings.TrimSpace(cfg.OSSSHKeyPath)
	if keyPath != "" {
		signer, err := loadPrivateKey(keyPath)
		if err != nil {
			return nil, err
		}
		return ssh.PublicKeys(signer), nil
	}
	password := strings.TrimSpace(cfg.OSPassword)
	if password == "" {
		return nil, fmt.Errorf("missing remote OS authentication, provide --os-password or --os-ssh-key-path")
	}
	return ssh.Password(password), nil
}

func loadPrivateKey(path string) (ssh.Signer, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ssh private key failed: %w", err)
	}
	signer, err := ssh.ParsePrivateKey(content)
	if err != nil {
		return nil, fmt.Errorf("parse ssh private key failed: %w", err)
	}
	return signer, nil
}

func (r *execRunner) Run(command string) (string, error) {
	session, err := r.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("ssh session create failed: %w", err)
	}
	defer session.Close()
	return runCommand(session, command)
}

func (r *execRunner) UploadExecutable(name string, content []byte) (string, error) {
	remotePath := path.Join("/tmp", fmt.Sprintf("%s-%d", name, time.Now().UnixNano()))
	file, err := r.sftp.Create(remotePath)
	if err != nil {
		return "", fmt.Errorf("create remote file failed: %w", err)
	}
	if _, err := file.Write(content); err != nil {
		file.Close()
		return "", fmt.Errorf("write remote file failed: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close remote file failed: %w", err)
	}
	if err := r.sftp.Chmod(remotePath, 0o755); err != nil {
		return "", fmt.Errorf("chmod remote file failed: %w", err)
	}
	return remotePath, nil
}

func (r *execRunner) RunExecutable(remotePath string) (string, error) {
	session, err := r.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("ssh session create failed: %w", err)
	}
	defer session.Close()
	return runCommand(session, remotePath)
}

func (r *execRunner) Remove(remotePath string) error {
	if err := r.sftp.Remove(remotePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove remote file failed: %w", err)
	}
	return nil
}

func (r *execRunner) Close() error {
	var closeErr error
	if r.sftp != nil {
		closeErr = r.sftp.Close()
	}
	if err := r.client.Close(); err != nil && closeErr == nil {
		closeErr = err
	}
	return closeErr
}

func runCommand(session *ssh.Session, command string) (string, error) {
	var stdout strings.Builder
	var stderr strings.Builder
	session.Stdout = &stdout
	session.Stderr = &stderr
	err := session.Run(command)
	if err != nil {
		return "", fmt.Errorf("remote command failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
