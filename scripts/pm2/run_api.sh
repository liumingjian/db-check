#!/usr/bin/env bash
set -euo pipefail

# PM2 entrypoint for db-web backend.
# Supports dev (go run) and production (prefer compiled bin/db-web if present).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

MODE="${DBCHECK_MODE:-dev}"
ADDR="${DBCHECK_ADDR:-127.0.0.1:8080}"

PYTHON_BIN="${DBCHECK_PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "${ROOT_DIR}/.venv/bin/python3" ]]; then
    PYTHON_BIN="${ROOT_DIR}/.venv/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

cd "${ROOT_DIR}"

if [[ "${MODE}" == "production" || "${MODE}" == "prod" ]]; then
  if [[ -x "${ROOT_DIR}/bin/db-web" ]]; then
    exec "${ROOT_DIR}/bin/db-web" --addr "${ADDR}" --python-bin "${PYTHON_BIN}"
  fi
fi

exec go run ./reporter/cmd/db-web --addr "${ADDR}" --python-bin "${PYTHON_BIN}"

