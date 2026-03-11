SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

ROOT_DIR := $(abspath .)
TMP_BIN_DIR := $(ROOT_DIR)/bin
GO_CACHE ?= /tmp/go-cache
COLLECTOR_BIN := $(TMP_BIN_DIR)/db-collector
REPORTER_BIN := $(TMP_BIN_DIR)/db-reporter

.PHONY: help init-python build build-collector build-reporter test-reporter test-integration test-e2e release clean

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

build-collector: ## 编译 db-collector 到 bin
	@mkdir -p "$(TMP_BIN_DIR)"
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
