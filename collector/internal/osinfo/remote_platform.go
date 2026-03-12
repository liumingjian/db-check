package osinfo

import (
	"fmt"
	"strings"
)

const (
	remoteKernelCommand = "uname -s"
	remoteArchCommand   = "uname -m"
)

type remotePlatform struct {
	GOOS   string
	GOARCH string
}

func detectRemotePlatform(runner remoteRunner) (remotePlatform, error) {
	kernel, err := runner.Run(remoteKernelCommand)
	if err != nil {
		return remotePlatform{}, fmt.Errorf("detect remote os failed: %w", err)
	}
	if strings.ToLower(strings.TrimSpace(kernel)) != "linux" {
		return remotePlatform{}, fmt.Errorf("remote OS collection only supports Linux over SSH")
	}
	archRaw, err := runner.Run(remoteArchCommand)
	if err != nil {
		return remotePlatform{}, fmt.Errorf("detect remote arch failed: %w", err)
	}
	goarch, err := normalizeRemoteArch(strings.TrimSpace(archRaw))
	if err != nil {
		return remotePlatform{}, err
	}
	return remotePlatform{GOOS: "linux", GOARCH: goarch}, nil
}

func normalizeRemoteArch(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "x86_64", "amd64":
		return "amd64", nil
	case "aarch64", "arm64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("unsupported remote architecture: %s", raw)
	}
}
