#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

API_BASE="${DBCHECK_SMOKE_API_BASE:-http://127.0.0.1:8080}"
WEB_BASE="${DBCHECK_SMOKE_WEB_BASE:-http://127.0.0.1:3000}"
TOKEN="${DBCHECK_SMOKE_TOKEN:-${DBCHECK_API_TOKEN:-secret}}"
TIMEOUT_SECONDS="${DBCHECK_SMOKE_TIMEOUT_SECONDS:-300}"

ZIP_PATH="${DBCHECK_SMOKE_ZIP_PATH:-/tmp/dbcheck-mysql-e2e.zip}"
DOWNLOAD_PATH="${DBCHECK_SMOKE_DOWNLOAD_PATH:-/tmp/dbcheck-reports.zip}"

require_cmd() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "[ERROR] missing command: ${name}" >&2
    exit 1
  fi
}

wait_for_http() {
  local url="$1"
  local expect_desc="$2"
  local deadline="$((SECONDS + TIMEOUT_SECONDS))"
  while true; do
    if curl -fsS -o /dev/null "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "[ERROR] timeout waiting for ${expect_desc}: ${url}" >&2
      return 1
    fi
    sleep 1
  done
}

pm2_has_app() {
  local name="$1"
  pm2 describe "${name}" >/dev/null 2>&1
}

maybe_start_pm2() {
  if pm2_has_app "dbcheck-api" && pm2_has_app "dbcheck-web"; then
    return 0
  fi
  echo "[INFO] starting PM2 apps from ecosystem.config.cjs"
  pm2 start "${ROOT_DIR}/ecosystem.config.cjs" >/dev/null
}

require_cmd pm2
require_cmd curl
require_cmd find
require_cmd sort
require_cmd tail
require_cmd xargs
require_cmd zip
require_cmd python3

maybe_start_pm2

echo "[INFO] waiting for frontend: ${WEB_BASE}"
wait_for_http "${WEB_BASE}/" "frontend"

echo "[INFO] waiting for backend: ${API_BASE}"
deadline="$((SECONDS + TIMEOUT_SECONDS))"
while true; do
  # Use a valid-looking task id so the handler reaches TaskStore (404 is OK).
  code="$(
    curl -sS -o /dev/null -w "%{http_code}" \
      -H "Authorization: Bearer ${TOKEN}" \
      "${API_BASE}/api/reports/status/00000000000000000000000000000000" || true
  )"
  case "${code}" in
    404) break ;;
    200) break ;;
    401)
      echo "[ERROR] backend reachable but token invalid (401). Set DBCHECK_SMOKE_TOKEN / DBCHECK_API_TOKEN." >&2
      exit 1
      ;;
    000|"")
      ;;
    *)
      echo "[WARN] backend not ready yet (HTTP ${code})" >&2
      ;;
  esac
  if ((SECONDS >= deadline)); then
    echo "[ERROR] timeout waiting for backend: ${API_BASE}" >&2
    exit 1
  fi
  sleep 1
done

RUN_DIR="$(
  find "${ROOT_DIR}/tests/e2e/runs" -maxdepth 4 -path '*/mysql-8.0/*/manifest.json' -print \
    | sort \
    | tail -n 1 \
    | xargs -I{} dirname {}
)"
if [[ -z "${RUN_DIR}" || ! -f "${RUN_DIR}/manifest.json" || ! -f "${RUN_DIR}/result.json" ]]; then
  echo "[ERROR] failed to locate mysql e2e run dir under tests/e2e/runs (need manifest.json + result.json)" >&2
  exit 1
fi

rm -f "${ZIP_PATH}"
zip -j "${ZIP_PATH}" "${RUN_DIR}/manifest.json" "${RUN_DIR}/result.json" >/dev/null
echo "[INFO] built input zip: ${ZIP_PATH}"

resp="$(
  curl -sS -X POST \
    -H "Authorization: Bearer ${TOKEN}" \
    -F "zips=@${ZIP_PATH};type=application/zip" \
    "${API_BASE}/api/reports/generate"
)"
task_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["task_id"])' <<<"${resp}")"

echo "[INFO] task_id=${task_id}"

status_deadline="$((SECONDS + TIMEOUT_SECONDS))"
download_url=""
while true; do
  status_json="$(
    curl -sS \
      -H "Authorization: Bearer ${TOKEN}" \
      "${API_BASE}/api/reports/status/${task_id}"
  )"
  status="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",""))' <<<"${status_json}")"

  case "${status}" in
    done)
      download_url="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("download_url",""))' <<<"${status_json}")"
      break
      ;;
    failed)
      err="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("error",""))' <<<"${status_json}")"
      echo "[ERROR] task failed: ${err}" >&2
      exit 1
      ;;
    processing|queued|"")
      ;;
    *)
      echo "[WARN] unexpected status: ${status}" >&2
      ;;
  esac

  if ((SECONDS >= status_deadline)); then
    echo "[ERROR] timeout waiting for task to finish (last status=${status})" >&2
    exit 1
  fi
  sleep 2
done

if [[ -z "${download_url}" ]]; then
  echo "[ERROR] task done but download_url missing" >&2
  exit 1
fi

rm -f "${DOWNLOAD_PATH}"
curl -sS \
  -H "Authorization: Bearer ${TOKEN}" \
  -o "${DOWNLOAD_PATH}" \
  "${API_BASE}${download_url}"
echo "[INFO] downloaded result zip: ${DOWNLOAD_PATH}"

python3 - <<'PY' "${DOWNLOAD_PATH}"
import sys, zipfile
path = sys.argv[1]
with zipfile.ZipFile(path) as zf:
  names = zf.namelist()
  if not any(n.lower().endswith(".docx") for n in names):
    raise SystemExit(f"no .docx found in result zip: {names[:20]}")
print("[INFO] result zip looks OK (contains .docx)")
PY

echo "[OK] smoke test passed"
