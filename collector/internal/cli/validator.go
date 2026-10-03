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
	if cfg.Local && state.SSHFlagsProvided {
		return errors.New("--local 与 SSH 参数互斥")
	}
	return nil
}

func validateCollectConfig(cfg Config, state parsedState) error {
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
	if hasOSSampling(state) && !cfg.Local && !cfg.OSOnly && !state.SSHFlagsProvided {
		return errors.New("OS 采样控制参数必须搭配 --local、--os-only 或远程 OS 参数")
	}
	return nil
}

func hasOSSampling(state parsedState) bool {
	return state.IntervalChanged || state.DurationChanged || state.CountChanged
}

func validateDBRequirements(cfg Config) error {
	service := strings.TrimSpace(cfg.OracleServiceName)
	if cfg.DBType != "oracle" && (service != "" || cfg.OracleSYSDBA) {
		return errors.New("--oracle-service-name 和 --oracle-sysdba 仅适用于 Oracle")
	}
	if cfg.DBType == "oracle" && service != "" && strings.TrimSpace(cfg.DBName) != "" {
		return errors.New("--dbname 与 --oracle-service-name 互斥")
	}
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
	if cfg.DBType == "oracle" && service != "" {
		return nil
	}
	if strings.TrimSpace(cfg.DBName) == "" {
		if cfg.DBType == "oracle" {
			return errors.New("缺少 --dbname 或 --oracle-service-name")
		}
		return errors.New("缺少 --dbname")
	}
	return nil
}
