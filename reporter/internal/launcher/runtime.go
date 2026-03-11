package launcher

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

const minPythonVersion = "3.10"

func VerifyPythonRuntime(pythonBin string, requirements string) error {
	if _, err := exec.LookPath(pythonBin); err != nil {
		return fmt.Errorf("未找到 Python 可执行文件 %q，请安装 Python %s+ 或使用 --python-bin 指定路径", pythonBin, minPythonVersion)
	}
	if err := checkPythonVersion(pythonBin); err != nil {
		return err
	}
	if err := checkPythonDeps(pythonBin, requirements); err != nil {
		return err
	}
	return nil
}

func checkPythonVersion(pythonBin string) error {
	code := "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)"
	cmd := exec.Command(pythonBin, "-c", code)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Python 版本过低，要求 >= %s", minPythonVersion)
	}
	return nil
}

func checkPythonDeps(pythonBin string, requirements string) error {
	code := "import jsonschema, docx"
	cmd := exec.Command(pythonBin, "-c", code)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	msg := strings.TrimSpace(string(output))
	if msg == "" {
		msg = "缺少 jsonschema 或 python-docx"
	}
	return fmt.Errorf("%s；请执行 `%s -m pip install -r %s`", msg, pythonBin, requirements)
}

func RunOrchestrator(pythonBin string, script string, args []string) error {
	command := exec.Command(pythonBin, append([]string{script}, args...)...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}
