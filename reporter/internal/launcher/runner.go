package launcher

import (
	"fmt"
	"os/exec"
)

const (
	ExitSuccess      = 0
	ExitParamError   = 2
	ExitRuntimeError = 20
	ExitValidate     = 30
	ExitReportError  = 40
)

func Execute(executablePath string, cfg Config) int {
	layout, err := ResolveAssetLayout(executablePath, cfg)
	if err != nil {
		fmt.Fprintf(stderr(), "[ERROR] %v\n", err)
		return ExitRuntimeError
	}
	if err := VerifyPythonRuntime(cfg.PythonBin, layout.Requirements); err != nil {
		fmt.Fprintf(stderr(), "[ERROR] %v\n", err)
		return ExitRuntimeError
	}
	if err := RunOrchestrator(cfg.PythonBin, layout.Script, orchestratorArgs(cfg, layout)); err != nil {
		return mapExecError(err)
	}
	return ExitSuccess
}

func orchestratorArgs(cfg Config, layout AssetLayout) []string {
	args := []string{
		"--run-dir", cfg.RunDir,
		"--rule-file", layout.RuleFile,
		"--template-file", layout.TemplateFile,
		"--out-docx", cfg.OutDocx,
		"--document-name", documentName(cfg),
		"--inspector", cfg.Inspector,
		"--change-description", cfg.ChangeDescription,
		"--review-name", cfg.ReviewName,
		"--review-title", cfg.ReviewTitle,
		"--review-contact", cfg.ReviewContact,
		"--review-email", cfg.ReviewEmail,
	}
	if cfg.OutMD != "" {
		args = append(args, "--out-md", cfg.OutMD)
	}
	if cfg.MySQLVersion != "" {
		args = append(args, "--mysql-version", cfg.MySQLVersion)
	}
	return args
}

func documentName(cfg Config) string {
	if cfg.DocumentName != "" {
		return cfg.DocumentName
	}
	return pathBase(cfg.OutDocx)
}

func mapExecError(err error) int {
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		fmt.Fprintf(stderr(), "[ERROR] 启动 orchestrator 失败: %v\n", err)
		return ExitRuntimeError
	}
	code := exitErr.ExitCode()
	switch code {
	case ExitParamError, ExitRuntimeError, ExitValidate, ExitReportError:
		return code
	default:
		return ExitReportError
	}
}
