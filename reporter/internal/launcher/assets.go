package launcher

import (
	"fmt"
	"os"
	"path/filepath"
)

type AssetLayout struct {
	Script       string
	RuleFile     string
	TemplateFile string
	Requirements string
}

func ResolveAssetLayout(executablePath string, cfg Config) (AssetLayout, error) {
	layout, err := discoverLayout(filepath.Dir(executablePath))
	if err != nil {
		return AssetLayout{}, err
	}
	if cfg.RuleFile == "" {
		dbType, typeErr := DetectRunDirDBType(cfg.RunDir)
		if typeErr != nil {
			return AssetLayout{}, typeErr
		}
		layout.RuleFile = rulePathForDBType(layout.RuleFile, dbType)
	}
	if cfg.RuleFile != "" {
		layout.RuleFile = filepath.Clean(cfg.RuleFile)
	}
	if cfg.TemplateFile != "" {
		layout.TemplateFile = filepath.Clean(cfg.TemplateFile)
	}
	return validateLayout(layout)
}

func discoverLayout(exeDir string) (AssetLayout, error) {
	candidates := []AssetLayout{
		releaseLayout(exeDir),
		repoLayout(exeDir),
		cwdLayout(),
	}
	for _, candidate := range candidates {
		if fileExists(candidate.Script) {
			return candidate, nil
		}
	}
	return AssetLayout{}, fmt.Errorf("未找到 reporter runtime，请检查安装包结构或源码目录")
}

func releaseLayout(exeDir string) AssetLayout {
	return AssetLayout{
		Script:       filepath.Join(exeDir, "runtime", "reporter_orchestrator.py"),
		RuleFile:     filepath.Join(exeDir, "assets", "rules", "mysql", "rule.json"),
		TemplateFile: filepath.Join(exeDir, "assets", "templates", "mysql-template.docx"),
		Requirements: filepath.Join(exeDir, "runtime", "requirements.txt"),
	}
}

func repoLayout(exeDir string) AssetLayout {
	root := filepath.Clean(filepath.Join(exeDir, ".."))
	return rootLayout(root)
}

func cwdLayout() AssetLayout {
	wd, err := os.Getwd()
	if err != nil {
		return AssetLayout{}
	}
	return rootLayout(wd)
}

func rootLayout(root string) AssetLayout {
	return AssetLayout{
		Script:       filepath.Join(root, "reporter", "cli", "reporter_orchestrator.py"),
		RuleFile:     filepath.Join(root, "rules", "mysql", "rule.json"),
		TemplateFile: filepath.Join(root, "reporter", "templates", "mysql-template.docx"),
		Requirements: filepath.Join(root, "requirements.txt"),
	}
}

func validateLayout(layout AssetLayout) (AssetLayout, error) {
	missing := firstMissing(layout)
	if missing != "" {
		return AssetLayout{}, fmt.Errorf("运行资产缺失: %s", missing)
	}
	return layout, nil
}

func firstMissing(layout AssetLayout) string {
	switch {
	case !fileExists(layout.Script):
		return layout.Script
	case !fileExists(layout.RuleFile):
		return layout.RuleFile
	case !fileExists(layout.TemplateFile):
		return layout.TemplateFile
	default:
		return ""
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
