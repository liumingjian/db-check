package cli

import (
	"errors"
	"strings"
)

func validateCommon(cfg Config, state parsedState) error {
	if cfg.DBType != "mysql" && cfg.DBType != "oracle" && cfg.DBType != "gaussdb" {
		return errors.New("--db-type 仅允许 mysql、oracle 或 gaussdb")
	}
	if cfg.DBPort <= 0 {
		return errors.New("--db-port 必须 > 0")
	}
	if cfg.OSPort <= 0 {
		return errors.New("--os-port 必须 > 0")
	}
	if cfg.SQLTimeoutSeconds <= 0 {
		return errors.New("--sql-timeout 必须 > 0")
	}
	if cfg.TopN <= 0 {
		return errors.New("--top-n 必须 > 0")
	}
	if cfg.OSSkip && cfg.OSOnly {
		return errors.New("--os-skip 与 --os-only 互斥")
	}
	if cfg.Local && state.SSHFlagsProvided {
		return errors.New("--local 与 SSH 参数互斥")
	}
	if cfg.DBType == "gaussdb" && strings.TrimSpace(cfg.GaussUser) == "" {
		return errors.New("gaussdb 缺少 --gauss-user")
	}
	if cfg.DBType == "gaussdb" && strings.TrimSpace(cfg.GaussEnvFile) == "" {
		return errors.New("gaussdb 缺少 --gauss-env-file")
	}
	return nil
}

func validateCollectConfig(cfg Config, state parsedState) error {
	if cfg.OSSkip && (state.IntervalChanged || state.DurationChanged || state.CountChanged) {
		return errors.New("--os-skip 与 OS 采样控制参数互斥")
	}
	if cfg.OSCollectDuration > 0 && cfg.OSCollectCount > 0 {
		return errors.New("--os-collect-duration 与 --os-collect-count 互斥")
	}
	if cfg.OSCollectInterval > 0 {
		hasDuration := cfg.OSCollectDuration > 0
		hasCount := cfg.OSCollectCount > 0
		if hasDuration == hasCount {
			return errors.New("--os-collect-interval > 0 时，必须且只能搭配 duration 或 count 之一")
		}
	}
	if cfg.OSCollectInterval == 0 && (cfg.OSCollectDuration > 0 || cfg.OSCollectCount > 0) {
		return errors.New("配置了 duration/count 时必须显式设置 --os-collect-interval > 0")
	}
	return nil
}

func validateDBRequirements(cfg Config) error {
	if cfg.OSOnly {
		return nil
	}
	if !cfg.Local && strings.TrimSpace(cfg.DBHost) == "" {
		return errors.New("远程模式必须提供 --db-host")
	}
	if strings.TrimSpace(cfg.DBUsername) == "" {
		return errors.New("缺少 --db-username")
	}
	if strings.TrimSpace(cfg.DBPassword) == "" {
		return errors.New("缺少 --db-password")
	}
	if strings.TrimSpace(cfg.DBName) == "" {
		return errors.New("缺少 --dbname")
	}
	return nil
}
