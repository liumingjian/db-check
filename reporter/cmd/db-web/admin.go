package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"dbcheck/reporter/internal/store"
	"dbcheck/reporter/internal/users"
	"dbcheck/reporter/internal/web"
)

// adminCreate runs `db-web admin create --username <name> [--data-dir <dir>]`:
// it creates the first admin and prints a temporary password once.
func adminCreate(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("db-web admin create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	username := fs.String("username", "", "required; the admin's username")
	dataDir := fs.String("data-dir", "", "data directory (or env DBCHECK_DATA_DIR)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "[ERROR] 参数错误: %v\n", err)
		return web.ExitParamError
	}
	if *dataDir == "" {
		*dataDir = strings.TrimSpace(getenv("DBCHECK_DATA_DIR"))
	}
	name := strings.TrimSpace(*username)
	if name == "" || *dataDir == "" {
		fmt.Fprintln(stderr, "[ERROR] 用法: db-web admin create --username <name> [--data-dir <dir> 或 env DBCHECK_DATA_DIR]")
		return web.ExitParamError
	}

	db, err := store.Open(*dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "[ERROR] %v\n", err)
		return web.ExitRuntimeError
	}
	defer db.Close()
	password, err := users.CreateFirstAdmin(context.Background(), db, name, time.Now())
	if errors.Is(err, users.ErrAdminExists) {
		fmt.Fprintln(stderr, "[ERROR] 管理员已存在，拒绝创建。请由现有管理员在控制台管理账号。")
		return web.ExitRuntimeError
	}
	if err != nil {
		fmt.Fprintf(stderr, "[ERROR] %v\n", err)
		return web.ExitRuntimeError
	}
	fmt.Fprintf(stdout, "已创建管理员 %s\n临时密码: %s\n该密码只显示这一次，首次登录后必须修改。\n", name, password)
	return 0
}
