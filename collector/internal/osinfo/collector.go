package osinfo

import (
	"context"
	"dbcheck/collector/internal/cli"
)

type Collector struct {
	NewRemoteRunner func(cfg cli.Config) (remoteRunner, error)
}

type snapshot struct {
	timestamp string
	errors    []string
}

func (s *snapshot) addErr(scope string, err error) {
	if err == nil {
		return
	}
	s.errors = append(s.errors, scope+": "+err.Error())
}

type remoteRunner interface {
	Run(command string) (string, error)
	UploadExecutable(name string, content []byte) (string, error)
	RunExecutable(path string) (string, error)
	Remove(path string) error
	Close() error
}

func (c Collector) Collect(ctx context.Context, cfg cli.Config) (map[string]any, error) {
	if useRemoteCollection(cfg) {
		return c.collectRemote(ctx, cfg)
	}
	return collectLocal(ctx, cfg)
}

func useRemoteCollection(cfg cli.Config) bool {
	return cfg.UseRemoteOS
}
