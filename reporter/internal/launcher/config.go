package launcher

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
)

const (
	defaultPythonBin  = "python3"
	defaultReportName = "report.docx"
)

var errShowHelp = errors.New("show help")

func IsHelp(err error) bool {
	return errors.Is(err, errShowHelp)
}

type Config struct {
	RunDir            string
	RuleFile          string
	TemplateFile      string
	AWRFile           string
	WDRFile           string
	OutDocx           string
	OutMD             string
	DocumentName      string
	Inspector         string
	ChangeDescription string
	ReviewName        string
	ReviewTitle       string
	ReviewContact     string
	ReviewEmail       string
	MySQLVersion      string
	PythonBin         string
	ShowHelp          bool
}

func ParseArgs(args []string) (Config, error) {
	cfg := defaultConfig()
	fs := newFlagSet(&cfg)
	if err := fs.Parse(args); err != nil {
		return Config{}, fmt.Errorf("参数错误: %w", err)
	}
	if cfg.ShowHelp {
		return cfg, errShowHelp
	}
	if cfg.RunDir == "" {
		return Config{}, errors.New("缺少 --run-dir")
	}
	cfg.RunDir = filepath.Clean(cfg.RunDir)
	if cfg.AWRFile != "" && cfg.WDRFile != "" {
		return Config{}, errors.New("--awr-file 与 --wdr-file 不能同时使用")
	}
	if cfg.AWRFile != "" && cfg.RuleFile != "" {
		return Config{}, errors.New("--awr-file 与 --rule-file 不能同时使用")
	}
	if cfg.WDRFile != "" && cfg.RuleFile != "" {
		return Config{}, errors.New("--wdr-file 与 --rule-file 不能同时使用")
	}
	if cfg.AWRFile != "" {
		cfg.AWRFile = filepath.Clean(cfg.AWRFile)
		dbType, err := detectRunDirDBType(cfg.RunDir)
		if err != nil {
			return Config{}, err
		}
		if dbType != "oracle" {
			return Config{}, fmt.Errorf("--awr-file 仅支持 Oracle run-dir，当前 db_type=%q", dbType)
		}
	}
	if cfg.WDRFile != "" {
		cfg.WDRFile = filepath.Clean(cfg.WDRFile)
		dbType, err := detectRunDirDBType(cfg.RunDir)
		if err != nil {
			return Config{}, err
		}
		if dbType != "gaussdb" {
			return Config{}, fmt.Errorf("--wdr-file 仅支持 GaussDB run-dir，当前 db_type=%q", dbType)
		}
	}
	cfg.OutDocx = normalizeDocxPath(cfg)
	return cfg, nil
}

func Usage() string {
	return `db-reporter --run-dir <run目录> [报告参数]

最短命令：
  db-reporter --run-dir ./runs/<run_id>

常用参数：
  --run-dir              采集输出目录，必须包含 manifest.json/result.json
  --document-name        报告文档名称，默认与输出 docx 文件名一致
  --inspector            巡检人员，默认 db-check
  --out-docx             Word 报告输出路径，默认 <run-dir>/report.docx
  --out-md               可选 Markdown 输出路径，默认不生成
  --rule-file            自定义规则文件，默认使用内置规则
  --awr-file             Oracle AWR HTML 报告路径（可选，仅 Oracle；与 --rule-file 互斥）
  --wdr-file             GaussDB WDR HTML 报告路径（可选，仅 GaussDB；与 --rule-file 互斥）
  --template-file        自定义 Word 模板，默认使用内置模板
  --python-bin           指定 Python 可执行文件，默认 python3
  --mysql-version        无法自动识别版本时手动指定数据库版本
  --help                 显示帮助
`
}

func defaultConfig() Config {
	return Config{
		DocumentName:      "",
		Inspector:         "db-check",
		ChangeDescription: "",
		ReviewName:        "周海波",
		ReviewTitle:       "数据库技术经理",
		ReviewContact:     "13570391044",
		ReviewEmail:       "haibo.zhou@antute.com.cn",
		PythonBin:         defaultPythonBin,
	}
}

func newFlagSet(cfg *Config) *flag.FlagSet {
	fs := flag.NewFlagSet("db-reporter", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())
	fs.StringVar(&cfg.RunDir, "run-dir", "", "collector 产出的 run 目录")
	fs.StringVar(&cfg.RuleFile, "rule-file", "", "自定义 rule.json")
	fs.StringVar(&cfg.TemplateFile, "template-file", "", "自定义 docx 模板")
	fs.StringVar(&cfg.AWRFile, "awr-file", "", "Oracle AWR HTML 报告路径（可选，仅 Oracle）")
	fs.StringVar(&cfg.WDRFile, "wdr-file", "", "GaussDB WDR HTML 报告路径（可选，仅 GaussDB）")
	fs.StringVar(&cfg.OutDocx, "out-docx", "", "输出 docx 路径")
	fs.StringVar(&cfg.OutMD, "out-md", "", "可选 markdown 输出路径")
	fs.StringVar(&cfg.DocumentName, "document-name", "", "文档名称")
	fs.StringVar(&cfg.Inspector, "inspector", cfg.Inspector, "巡检人员")
	fs.StringVar(&cfg.ChangeDescription, "change-description", cfg.ChangeDescription, "修订记录描述")
	fs.StringVar(&cfg.ReviewName, "review-name", cfg.ReviewName, "审阅人姓名")
	fs.StringVar(&cfg.ReviewTitle, "review-title", cfg.ReviewTitle, "审阅人职务")
	fs.StringVar(&cfg.ReviewContact, "review-contact", cfg.ReviewContact, "审阅人联系方式")
	fs.StringVar(&cfg.ReviewEmail, "review-email", cfg.ReviewEmail, "审阅人邮箱")
	fs.StringVar(&cfg.MySQLVersion, "mysql-version", "", "手动指定 MySQL 版本")
	fs.StringVar(&cfg.PythonBin, "python-bin", cfg.PythonBin, "Python 可执行文件")
	fs.BoolVar(&cfg.ShowHelp, "help", false, "显示帮助")
	return fs
}

func normalizeDocxPath(cfg Config) string {
	if cfg.OutDocx != "" {
		return filepath.Clean(cfg.OutDocx)
	}
	return filepath.Join(cfg.RunDir, defaultReportName)
}
