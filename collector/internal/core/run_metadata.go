package core

import (
	"dbcheck/collector/internal/cli"
	"fmt"
	"strings"
	"time"
)

func overallStatus(exitCode int) string {
	switch exitCode {
	case ExitSuccess:
		return "success"
	case ExitPartial:
		return "partial_success"
	default:
		return "failed"
	}
}

func buildRunID(dbType string, host string, started time.Time) string {
	return fmt.Sprintf("%s-%s-%s", dbType, sanitizeRunHost(host), started.UTC().Format("20060102T150405Z"))
}

func hostForRunID(cfg cli.Config) string {
	if cfg.DBHost != "" {
		return cfg.DBHost
	}
	if cfg.Local {
		return "localhost"
	}
	return "unknown"
}

func hostForResult(cfg cli.Config) string {
	if cfg.DBHost != "" {
		return cfg.DBHost
	}
	return "localhost"
}

func sanitizeRunHost(host string) string {
	host = strings.TrimSpace(host)
	host = strings.ReplaceAll(host, ":", "_")
	host = strings.ReplaceAll(host, "/", "_")
	if host == "" {
		return "unknown"
	}
	return host
}

func durationMS(start time.Time, end time.Time) int64 {
	if end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}
