package main

import (
	"dbcheck/reporter/internal/launcher"
	"fmt"
	"os"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := launcher.ParseArgs(os.Args[1:])
	if err != nil {
		if launcher.IsHelp(err) {
			fmt.Print(launcher.Usage())
			return launcher.ExitSuccess
		}
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		fmt.Fprint(os.Stderr, launcher.Usage())
		return launcher.ExitParamError
	}
	executablePath, pathErr := os.Executable()
	if pathErr != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] 获取程序路径失败: %v\n", pathErr)
		return launcher.ExitRuntimeError
	}
	return launcher.Execute(executablePath, cfg)
}
