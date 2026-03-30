SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

ROOT_DIR := $(abspath .)
TMP_BIN_DIR := $(ROOT_DIR)/bin
GO_CACHE ?= /tmp/go-cache
COLLECTOR_BIN := $(TMP_BIN_DIR)/db-collector
REPORTER_BIN := $(TMP_BIN_DIR)/db-reporter

.PHONY: help init-python build build-collector build-reporter build-osprobes test-reporter test-integration test-e2e release clean \
	web-install web-build build-db-web \
	pm2-start pm2-start-prod pm2-restart pm2-stop pm2-delete pm2-status pm2-logs pm2-logs-api pm2-logs-web pm2-smoke

help: ## 显示可用目标与示例
	@printf "db-check Make targets\n\n"
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf "\n示例:\n"
	@printf "  make init-python\n"
	@printf "  make build\n"
	@printf "  ./bin/db-collector --db-type mysql --db-host 127.0.0.1 --db-port 3306 --db-username root --db-password rootpwd --dbname mysql --output-dir ./runs\n"
	@printf "  ./bin/db-reporter --python-bin \"$$VIRTUAL_ENV/bin/python3\" --run-dir ./runs/<run_id>\n"
	@printf "  GOCACHE=$(GO_CACHE) go run ./collector/cmd/db-collector --db-type mysql --os-only --output-dir ./runs\n"
	@printf "  GOCACHE=$(GO_CACHE) go run ./reporter/cmd/db-reporter --python-bin \"$$VIRTUAL_ENV/bin/python3\" --run-dir ./runs/<run_id>\n"

init-python: ## 在已激活的 .venv 中安装 Python 依赖
	@test -n "$$VIRTUAL_ENV" || { echo "[ERROR] python3 must run inside an activated virtual environment (VIRTUAL_ENV is empty)" >&2; exit 1; }
	@./scripts/init_python_env.sh

build: build-collector build-reporter ## 编译两个客户入口到 bin

build-osprobes: ## 生成嵌入式远程 OS helper 资产
	@./scripts/build_embedded_osprobes.sh
	@printf "built embedded os probes\n"

build-collector: ## 编译 db-collector 到 bin
	@mkdir -p "$(TMP_BIN_DIR)"
	@./scripts/build_embedded_osprobes.sh
	@GOCACHE=$(GO_CACHE) go build -o "$(COLLECTOR_BIN)" ./collector/cmd/db-collector
	@printf "built %s\n" "$(COLLECTOR_BIN)"

build-reporter: ## 编译 db-reporter 到 bin
	@mkdir -p "$(TMP_BIN_DIR)"
	@GOCACHE=$(GO_CACHE) go build -o "$(REPORTER_BIN)" ./reporter/cmd/db-reporter
	@printf "built %s\n" "$(REPORTER_BIN)"

test-reporter: ## 在 .venv 中运行 reporter 单元测试
	@test -n "$$VIRTUAL_ENV" || { echo "[ERROR] python3 must run inside an activated virtual environment (VIRTUAL_ENV is empty)" >&2; exit 1; }
	@python3 -m unittest discover -s tests/reporter -p 'test_*.py'

test-integration: ## 在 .venv 中运行集成测试
	@test -n "$$VIRTUAL_ENV" || { echo "[ERROR] python3 must run inside an activated virtual environment (VIRTUAL_ENV is empty)" >&2; exit 1; }
	@python3 -m unittest discover -s tests/integration -p 'test_*.py'

test-e2e: ## 在 .venv 中运行 Docker e2e
	@test -n "$$VIRTUAL_ENV" || { echo "[ERROR] python3 must run inside an activated virtual environment (VIRTUAL_ENV is empty)" >&2; exit 1; }
	@tests/e2e/run_docker_e2e.sh

release: ## 构建多平台发布包到 dist/
	@./scripts/build_release_packages.sh

clean: ## 清理本地临时构建产物
	@rm -rf "$(TMP_BIN_DIR)"
	@printf "removed %s\n" "$(TMP_BIN_DIR)"

web-install: ## 安装 web/ 前端依赖（npm install）
	@cd web && npm install

web-build: ## 构建 web/ 前端产物（next build）
	@cd web && npm run build

build-db-web: ## 编译 db-web 后端到 bin/db-web（供 PM2 production 优先使用）
	@mkdir -p "$(TMP_BIN_DIR)"
	@GOCACHE=$(GO_CACHE) go build -o "$(TMP_BIN_DIR)/db-web" ./reporter/cmd/db-web
	@printf "built %s\n" "$(TMP_BIN_DIR)/db-web"

pm2-start: ## 使用 PM2 启动 dbcheck-api + dbcheck-web（dev 模式）
	@pm2 start ecosystem.config.cjs
	@pm2 ls

pm2-start-prod: ## 使用 PM2 启动（production 模式；需先 web-build，推荐 build-db-web）
	@pm2 start ecosystem.config.cjs --env production
	@pm2 ls

pm2-restart: ## PM2 重启并刷新 env（等价于 --update-env）
	@pm2 restart ecosystem.config.cjs --update-env
	@pm2 ls

pm2-stop: ## PM2 停止 dbcheck-api + dbcheck-web（保留进程定义）
	@pm2 stop dbcheck-api dbcheck-web || true
	@pm2 ls

pm2-delete: ## PM2 删除 dbcheck-api + dbcheck-web（从进程列表移除）
	@pm2 delete dbcheck-api dbcheck-web || true
	@pm2 ls

pm2-status: ## 查看 PM2 进程状态
	@pm2 ls

pm2-logs: ## 查看 PM2 总日志（最近 200 行）
	@pm2 logs --lines 200

pm2-logs-api: ## 查看后端日志（dbcheck-api）
	@pm2 logs dbcheck-api --lines 200

pm2-logs-web: ## 查看前端日志（dbcheck-web）
	@pm2 logs dbcheck-web --lines 200

pm2-smoke: ## 最小链路 smoke test（curl 上传 e2e zip → 轮询 → 下载）
	@./scripts/pm2/smoke_test.sh
