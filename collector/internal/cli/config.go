package cli

import (
	"errors"
	"strings"
)

const (
	DefaultDBType            = "mysql"
	DefaultMySQLPort         = 3306
	DefaultOraclePort        = 1521
	DefaultOutputDir         = "./output"
	DefaultLogPath           = "./logs"
	DefaultLogLevel          = "INFO"
	DefaultSQLTimeoutSeconds = 180
	DefaultTopN              = 20
	DefaultOSPort            = 22
	Version                  = "1.1.0"
)

var ErrShowHelp = errors.New("show help")

type Config struct {
	DBType            string
	DBHost            string
	DBPort            int
	DBUsername        string
	DBPassword        string
	DBName            string
	Local             bool
	OSOnly            bool
	OSSkip            bool
	OutputDir         string
	LogPath           string
	LogLevel          string
	SQLTimeoutSeconds int
	TopN              int
	OSHost            string
	OSPort            int
	OSUsername        string
	OSPassword        string
	OSSSHKeyPath      string
	OSCollectInterval int
	OSCollectDuration int
	OSCollectCount    int
	UseRemoteOS       bool
	ShowVersion       bool
	ShowHelp          bool
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
	fillDerivedPort(&cfg)
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
	cfg.UseRemoteOS = shouldUseRemoteOS(cfg, state)
	return cfg, nil
}

func Usage() string {
	return `db-collector --db-type <mysql|oracle> [连接参数] [采集参数]

最短命令：
  db-collector --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password rootpwd --dbname dbcheck --output-dir ./runs
  db-collector --db-type oracle --db-host 127.0.0.1 --db-port 1521 --db-username system --db-password oraclepwd --dbname ORCL --output-dir ./runs

核心参数：
  --db-type/-t                数据库类型，支持 mysql / oracle
  --db-host/-h                数据库地址（远程模式必填）
  --db-port/-P                数据库端口，mysql 默认 3306，oracle 默认 1521
  --db-username/-u            数据库用户名（非 --os-only 必填）
  --db-password/-p            数据库密码（非 --os-only 必填）
  --dbname/-d                 数据库名；Oracle 路径下表示 SID/实例名（非 --os-only 必填）
  --local                     本地 OS 采集模式
  --os-only                   仅采集 OS（旁路）
  --os-skip                   跳过 OS 采集
  --output-dir/-o             输出目录

远程 OS 参数（Linux over SSH）：
  --os-host                   OS 主机地址，默认与 --db-host 相同
  --os-port                   SSH 端口，默认 22
  --os-username               SSH 用户名，默认与 --db-username 相同
  --os-password               SSH 密码，默认与 --db-password 相同
  --os-ssh-key-path           SSH 私钥路径

OS 采样参数：
  --os-collect-interval       OS 采样间隔（秒）
  --os-collect-duration       OS 采样时长（秒）
  --os-collect-count          OS 采样数量
  --version/-v                显示版本
  --help                      显示帮助
`
}

func defaultConfig() Config {
	return Config{
		DBType:            DefaultDBType,
		OutputDir:         DefaultOutputDir,
		LogPath:           DefaultLogPath,
		LogLevel:          DefaultLogLevel,
		SQLTimeoutSeconds: DefaultSQLTimeoutSeconds,
		TopN:              DefaultTopN,
		OSPort:            DefaultOSPort,
	}
}

func fillDerivedPort(cfg *Config) {
	if cfg.DBPort > 0 {
		return
	}
	cfg.DBPort = defaultDBPort(cfg.DBType)
}

func defaultDBPort(dbType string) int {
	if dbType == "oracle" {
		return DefaultOraclePort
	}
	return DefaultMySQLPort
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

func shouldUseRemoteOS(cfg Config, state parsedState) bool {
	if cfg.Local || cfg.OSSkip {
		return false
	}
	if state.SSHFlagsProvided {
		return true
	}
	return strings.TrimSpace(cfg.DBHost) != "" && !cfg.OSOnly
}
