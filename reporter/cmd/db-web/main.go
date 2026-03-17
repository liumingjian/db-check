package main

import (
	"dbcheck/reporter/internal/web"
	"fmt"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv))
}

func run(args []string, getenv func(string) string) int {
	cfg, err := web.ParseConfig(args, getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		return web.ExitParamError
	}
	if err := web.Run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		return web.ExitRuntimeError
	}
	return 0
}
