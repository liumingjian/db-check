package main

import (
	"context"
	"dbcheck/collector/internal/cli"
	"dbcheck/collector/internal/core"
	"dbcheck/collector/internal/gaussdb"
	"dbcheck/collector/internal/model"
	"dbcheck/collector/internal/mysql"
	"dbcheck/collector/internal/oracle"
	"dbcheck/collector/internal/osinfo"
	"dbcheck/collector/internal/output"
	"errors"
	"fmt"
	"os"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := cli.ParseArgs(os.Args[1:])
	if err != nil {
		if errors.Is(err, cli.ErrShowHelp) {
			fmt.Print(cli.Usage())
			return core.ExitSuccess
		}
		fmt.Fprintf(os.Stderr, "参数错误: %v\n", err)
		fmt.Fprint(os.Stderr, cli.Usage())
		return core.ExitPrecheckFailed
	}
	if cfg.ShowVersion {
		fmt.Println(cli.Version)
		return core.ExitSuccess
	}
	dbCollector, err := newDBCollector(cfg.DBType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化失败: %v\n", err)
		return core.ExitPrecheckFailed
	}
	runner, err := core.NewRunner(core.Dependencies{
		Clock:       model.RealClock{},
		DBCollector: dbCollector,
		OSCollector: osinfo.Collector{},
		Writer:      output.FileWriter{},
		Version:     cli.Version,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "初始化失败: %v\n", err)
		return core.ExitCollectFailed
	}
	artifacts, err := runner.Run(context.Background(), cfg)
	if err != nil {
		if typed, ok := err.(core.RunnerError); ok {
			fmt.Fprintf(os.Stderr, "执行失败: %v\n", typed.Err)
			return typed.ExitCode
		}
		fmt.Fprintf(os.Stderr, "执行失败: %v\n", err)
		return core.ExitCollectFailed
	}
	fmt.Printf("run_id=%s\n", artifacts.RunID)
	fmt.Printf("manifest=%s\n", artifacts.ManifestPath)
	if artifacts.Result != nil {
		fmt.Printf("result=%s\n", artifacts.ResultPath)
	}
	return artifacts.Manifest.ExitCode
}

func newDBCollector(dbType string) (core.DBCollector, error) {
	switch dbType {
	case "mysql":
		return mysql.Collector{}, nil
	case "oracle":
		return oracle.Collector{}, nil
	case "gaussdb":
		return gaussdb.Collector{}, nil
	default:
		return nil, fmt.Errorf("unsupported db-type: %s", dbType)
	}
}
