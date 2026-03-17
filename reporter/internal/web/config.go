package web

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"
)

const (
	ExitParamError   = 2
	ExitRuntimeError = 20
)

const (
	envDataDir        = "DBCHECK_DATA_DIR"
	envAllowedOrigins = "ALLOWED_ORIGINS"
	envAPIToken       = "DBCHECK_API_TOKEN"
)

const (
	defaultAddr           = ":8080"
	defaultMaxUploadBytes = int64(1_073_741_824) // 1 GiB
	defaultRetentionTTL   = 24 * time.Hour
	defaultLogReplayLines = 1000
	defaultPythonBin      = "python3"
)

type Config struct {
	Addr           string
	DataDir        string
	AllowedOrigins []string
	APIToken       string

	MaxUploadBytes int64
	RetentionTTL   time.Duration
	LogReplayLines int
	PythonBin      string
}

func ParseConfig(args []string, getenv func(string) string) (Config, error) {
	fs := flag.NewFlagSet("db-web", flag.ContinueOnError)
	fs.SetOutput(discardFlagOutput())

	var cfg Config
	fs.StringVar(&cfg.Addr, "addr", defaultAddr, "listen address (e.g. :8080)")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "required; base directory for task data (or env DBCHECK_DATA_DIR)")
	var allowedOrigins string
	fs.StringVar(&allowedOrigins, "allowed-origins", "", "required; comma-separated origin whitelist (or env ALLOWED_ORIGINS)")
	fs.Int64Var(&cfg.MaxUploadBytes, "max-upload-bytes", defaultMaxUploadBytes, "max upload size in bytes; 0 disables the limit")
	retentionTTL := fs.Duration("retention-ttl", defaultRetentionTTL, "task retention TTL; 0 disables auto cleanup")
	fs.IntVar(&cfg.LogReplayLines, "log-replay-lines", defaultLogReplayLines, "log replay lines on WS/status; 0 disables truncation")
	fs.StringVar(&cfg.PythonBin, "python-bin", defaultPythonBin, "python executable (default python3)")

	if err := fs.Parse(args); err != nil {
		return Config{}, fmt.Errorf("参数错误: %w", err)
	}

	if cfg.DataDir == "" {
		cfg.DataDir = strings.TrimSpace(getenv(envDataDir))
	}
	if allowedOrigins == "" {
		allowedOrigins = strings.TrimSpace(getenv(envAllowedOrigins))
	}
	cfg.AllowedOrigins = splitCSV(allowedOrigins)
	cfg.APIToken = strings.TrimSpace(getenv(envAPIToken))
	cfg.RetentionTTL = *retentionTTL

	if strings.TrimSpace(cfg.DataDir) == "" {
		return Config{}, errors.New("缺少 --data-dir 或 env DBCHECK_DATA_DIR")
	}
	if len(cfg.AllowedOrigins) == 0 {
		return Config{}, errors.New("缺少 --allowed-origins 或 env ALLOWED_ORIGINS")
	}
	if cfg.APIToken == "" {
		return Config{}, errors.New("缺少 env DBCHECK_API_TOKEN")
	}
	if cfg.LogReplayLines < 0 {
		return Config{}, errors.New("--log-replay-lines 不能为负数")
	}
	if cfg.MaxUploadBytes < 0 {
		return Config{}, errors.New("--max-upload-bytes 不能为负数")
	}
	if cfg.RetentionTTL < 0 {
		return Config{}, errors.New("--retention-ttl 不能为负数")
	}
	if strings.TrimSpace(cfg.PythonBin) == "" {
		return Config{}, errors.New("--python-bin 不能为空")
	}
	return cfg, nil
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
