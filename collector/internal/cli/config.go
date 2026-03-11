package cli

import (
	"errors"
	"strings"
)

const (
	DefaultDBType              = "mysql"
	DefaultDBPort              = 3306
	DefaultOutputDir           = "./output"
	DefaultLogPath             = "./logs"
	DefaultLogLevel            = "INFO"
	DefaultSQLTimeoutSeconds   = 180
	DefaultTopN                = 20
	DefaultOSPort              = 22
	DefaultRemoteCollectorPath = "/tmp/db-collector"
	Version                    = "2.0.0"
)

var ErrShowHelp = errors.New("show help")

type Config struct {
	DBType              string
	DBHost              string
	DBPort              int
	DBUsername          string
	DBPassword          string
	DBName              string
	Local               bool
	OSOnly              bool
	OSSkip              bool
	OutputDir           string
	LogPath             string
	LogLevel            string
	SQLTimeoutSeconds   int
	TopN                int
	OSHost              string
	OSPort              int
	OSUsername          string
	OSPassword          string
	OSSSHKeyPath        string
	RemoteCollectorPath string
	OSCollectInterval   int
	OSCollectDuration   int
	OSCollectCount      int
	ShowVersion         bool
	ShowHelp            bool
}

type parsedState struct {
	SSHFlagsProvided bool
	DurationChanged  bool
	CountChanged     bool
	IntervalChanged  bool
}

func ParseArgs(args []string) (Config, error) {
	cfg := defaultConfig()
	state, err := parseArgsIntoConfig(args, &cfg)
	if err != nil {
		return Config{}, err
	}
	if cfg.ShowHelp {
		return cfg, ErrShowHelp
	}
	if hasOracleInput(cfg, args) {
		return Config{}, errors.New("检测到 Oracle 专用输入，v2.0 仅支持 mysql，请先完成参数迁移")
	}
	if err := validateCommon(cfg, state); err != nil {
		return Config{}, err
	}
	if err := validateCollectConfig(cfg, state); err != nil {
		return Config{}, err
	}
	if err := validateDBRequirements(cfg); err != nil {
		return Config{}, err
	}
	fillDerivedDefaults(&cfg)
	return cfg, nil
}

func Usage() string {
	return `db-collector --db-type mysql [连接参数] [采集参数]

核心参数：
  --db-type/-t                数据库类型，仅支持 mysql
  --db-host/-h                数据库地址（远程模式必填）
  --db-port/-P                数据库端口，默认 3306
  --db-username/-u            数据库用户名（非 --os-only 必填）
  --db-password/-p            数据库密码（非 --os-only 必填）
  --dbname/-d                 数据库名（非 --os-only 必填）
  --local                     本地模式
  --os-only                   仅采集 OS（旁路）
  --os-skip                   跳过 OS 采集
  --output-dir/-o             输出目录
  --os-collect-interval       OS 采样间隔（秒）
  --os-collect-duration       OS 采样时长（秒）
  --os-collect-count          OS 采样数量
  --version/-v                显示版本
  --help                      显示帮助
`
}

func defaultConfig() Config {
	return Config{
		DBType:              DefaultDBType,
		DBPort:              DefaultDBPort,
		OutputDir:           DefaultOutputDir,
		LogPath:             DefaultLogPath,
		LogLevel:            DefaultLogLevel,
		SQLTimeoutSeconds:   DefaultSQLTimeoutSeconds,
		TopN:                DefaultTopN,
		OSPort:              DefaultOSPort,
		RemoteCollectorPath: DefaultRemoteCollectorPath,
	}
}

func hasOracleInput(cfg Config, args []string) bool {
	if strings.EqualFold(cfg.DBType, "oracle") {
		return true
	}
	for _, arg := range args {
		if strings.Contains(strings.ToLower(arg), "oracle") {
			return true
		}
	}
	return false
}

func fillDerivedDefaults(cfg *Config) {
	if cfg.DBHost == "" {
		cfg.DBHost = "127.0.0.1"
	}
	if cfg.OSHost == "" {
		cfg.OSHost = cfg.DBHost
	}
	if cfg.OSUsername == "" {
		cfg.OSUsername = cfg.DBUsername
	}
	if cfg.OSPassword == "" {
		cfg.OSPassword = cfg.DBPassword
	}
}
