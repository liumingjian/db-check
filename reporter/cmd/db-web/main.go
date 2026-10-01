package main

import (
	"dbcheck/reporter/internal/web"
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) >= 2 && args[0] == "admin" && args[1] == "create" {
		return adminCreate(args[2:], getenv, stdout, stderr)
	}
	cfg, err := web.ParseConfig(args, getenv)
	if err != nil {
		fmt.Fprintf(stderr, "[ERROR] %v\n", err)
		return web.ExitParamError
	}
	if err := web.Run(cfg); err != nil {
		fmt.Fprintf(stderr, "[ERROR] %v\n", err)
		return web.ExitRuntimeError
	}
	return 0
}
