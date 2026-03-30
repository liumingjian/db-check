#!/usr/bin/env bash
set -euo pipefail

# PM2 entrypoint for Next.js frontend in web/.
# Supports dev (next dev) and production (next start; requires prior build).

MODE="${DBCHECK_MODE:-dev}"
PORT="${PORT:-3000}"

if [[ "${MODE}" == "production" || "${MODE}" == "prod" ]]; then
  if [[ ! -d ".next" ]]; then
    echo "[ERROR] web/.next/ not found. Run: cd web && npm run build" >&2
    exit 1
  fi
  exec npm run start -- -p "${PORT}"
fi

exec npm run dev -- -p "${PORT}"

