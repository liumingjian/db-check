#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BASE_COMPOSE_FILE="$ROOT_DIR/tests/e2e/docker/docker-compose.yml"
RULE_FILE="$ROOT_DIR/rules/mysql/rule.json"
TEMPLATE_FILE="$ROOT_DIR/reporter/templates/mysql-template.docx"
SCENARIO_SCRIPT="$ROOT_DIR/tests/e2e/docker/mysql/apply_scenarios.sh"
RUNS_ROOT="$ROOT_DIR/tests/e2e/runs"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUTPUT_DIR="$RUNS_ROOT/$TIMESTAMP"
SUPPORTED_VERSIONS=("5.6" "5.7" "8.0")
REQUESTED_VERSIONS=()

assert_commands() {
  command -v docker >/dev/null
  docker compose version >/dev/null
  if ! docker info >/dev/null 2>&1; then
    echo "[ERROR] docker daemon is not accessible in current environment" >&2
    exit 1
  fi
  command -v go >/dev/null
  command -v python3 >/dev/null
  test -x "$SCENARIO_SCRIPT"
}

assert_venv() {
  if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    echo "[ERROR] python3 must run inside an activated virtual environment (VIRTUAL_ENV is empty)" >&2
    exit 1
  fi
}

assert_python_deps() {
  if ! python3 - <<'PY' >/dev/null 2>&1
import jsonschema  # noqa: F401
import docx  # noqa: F401
PY
  then
    echo "[ERROR] missing Python dependencies in current virtual environment; run scripts/init_python_env.sh first" >&2
    exit 1
  fi
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --mysql-version)
        REQUESTED_VERSIONS+=("${2:?missing mysql version}")
        shift 2
        ;;
      *)
        echo "[ERROR] unsupported argument: $1" >&2
        exit 1
        ;;
    esac
  done
  if ((${#REQUESTED_VERSIONS[@]} == 0)); then
    REQUESTED_VERSIONS=("${SUPPORTED_VERSIONS[@]}")
  fi
}

assert_version_supported() {
  local version="$1"
  local supported
  for supported in "${SUPPORTED_VERSIONS[@]}"; do
    if [[ "$supported" == "$version" ]]; then
      return 0
    fi
  done
  echo "[ERROR] unsupported MySQL version: $version" >&2
  exit 1
}

mysql_override_file() {
  case "$1" in
    5.6) printf '%s' "$ROOT_DIR/tests/e2e/docker/docker-compose.mysql56.yml" ;;
    5.7) printf '%s' "$ROOT_DIR/tests/e2e/docker/docker-compose.mysql57.yml" ;;
    8.0) printf '%s' "$ROOT_DIR/tests/e2e/docker/docker-compose.mysql80.yml" ;;
    *) return 1 ;;
  esac
}

mysql_host_port() {
  case "$1" in
    5.6) printf '%s' '13356' ;;
    5.7) printf '%s' '13357' ;;
    8.0) printf '%s' '13306' ;;
    *) return 1 ;;
  esac
}

version_slug() {
  printf '%s' "${1//./}"
}

cleanup_compose() {
  local project="$1"
  local override="$2"
  docker compose -p "$project" -f "$BASE_COMPOSE_FILE" -f "$override" down -v --remove-orphans >/dev/null 2>&1 || true
}

stop_runtime_scenarios() {
  local project="$1"
  local override="$2"
  "$SCENARIO_SCRIPT" stop-runtime "$project" "$override" >/dev/null 2>&1 || true
}

run_collector() {
  local db_port="$1"
  local output_dir="$2"
  "$ROOT_DIR/bin/db-collector" \
    --db-type mysql \
    --db-host 127.0.0.1 \
    --db-port "$db_port" \
    --db-username root \
    --db-password rootpwd \
    --dbname dbcheck \
    --output-dir "$output_dir"
}

build_collector() {
  echo "[INFO] building db-collector"
  mkdir -p "$ROOT_DIR/bin"
  GOCACHE=/tmp/go-cache go build -o "$ROOT_DIR/bin/db-collector" "$ROOT_DIR/collector/cmd/db-collector"
}

extract_run_id() {
  local stdout_text="$1"
  local run_id_line
  run_id_line="$(printf '%s\n' "$stdout_text" | awk -F= '/^run_id=/{print $2; exit}')"
  if [[ -z "$run_id_line" ]]; then
    echo "[ERROR] run_id not found in collector output" >&2
    echo "$stdout_text" >&2
    exit 1
  fi
  printf '%s' "$run_id_line"
}

run_version_e2e() (
  set -euo pipefail
  local version="$1"
  local override
  local db_port
  local project
  local version_output_dir
  local collector_output
  local run_id
  local run_dir
  local manifest
  local result
  local summary
  local report_meta
  local report_md
  local report_view
  local report
  local runtime_started=0

  cleanup_version() {
    if [[ "$runtime_started" == "1" ]]; then
      stop_runtime_scenarios "$project" "$override"
    fi
    cleanup_compose "$project" "$override"
  }

  override="$(mysql_override_file "$version")"
  db_port="$(mysql_host_port "$version")"
  project="dbcheck-e2e-$(version_slug "$version")"
  version_output_dir="$OUTPUT_DIR/mysql-$version"
  mkdir -p "$version_output_dir"

  cleanup_compose "$project" "$override"
  trap cleanup_version EXIT

  echo "[INFO] starting docker services for MySQL $version"
  docker compose -p "$project" -f "$BASE_COMPOSE_FILE" -f "$override" up -d --wait

  echo "[INFO] applying mysql scenarios for MySQL $version"
  "$SCENARIO_SCRIPT" seed "$project" "$override"

  echo "[INFO] starting runtime contention for MySQL $version"
  "$SCENARIO_SCRIPT" start-runtime "$project" "$override"
  runtime_started=1

  echo "[INFO] running collector for MySQL $version"
  collector_output="$(GOCACHE=/tmp/go-cache run_collector "$db_port" "$version_output_dir")"
  printf '%s\n' "$collector_output"
  stop_runtime_scenarios "$project" "$override"
  runtime_started=0

  run_id="$(extract_run_id "$collector_output")"
  run_dir="$version_output_dir/$run_id"
  manifest="$run_dir/manifest.json"
  result="$run_dir/result.json"
  summary="$run_dir/summary.json"
  report_meta="$run_dir/report-meta.json"
  report_md="$run_dir/report.md"
  report_view="$run_dir/report-view.json"
  report="$run_dir/report.docx"

  echo "[INFO] running analyzer for MySQL $version"
  python3 "$ROOT_DIR/analyzer/cli/db_analyzer.py" \
    --manifest "$manifest" \
    --result "$result" \
    --rule "$RULE_FILE" \
    --strict-schema \
    --out "$summary"

  echo "[INFO] generating report meta for MySQL $version"
  python3 "$ROOT_DIR/reporter/cli/generate_report_meta.py" \
    --result "$result" \
    --summary "$summary" \
    --mysql-version "$version" \
    --out "$report_meta"

  echo "[INFO] generating markdown report for MySQL $version"
  python3 "$ROOT_DIR/reporter/cli/db_report_preview.py" \
    --result "$result" \
    --summary "$summary" \
    --meta "$report_meta" \
    --out-md "$report_md" \
    --out-json "$report_view"

  echo "[INFO] rendering template docx for MySQL $version"
  python3 "$ROOT_DIR/reporter/cli/render_template_docx.py" \
    --report-md "$report_md" \
    --report-view "$report_view" \
    --template "$TEMPLATE_FILE" \
    --out "$report"

  echo "[INFO] validating contracts for MySQL $version"
  python3 "$ROOT_DIR/tasks/validate_frozen_contracts.py" \
    --manifest "$manifest" \
    --result "$result" \
    --summary "$summary" \
    --rule "$RULE_FILE" \
    --strict-schema

  echo "[INFO] docker e2e succeeded for MySQL $version"
  echo "[INFO] artifacts[$version]: $run_dir"
)

main() {
  local version
  parse_args "$@"
  assert_commands
  assert_venv
  assert_python_deps
  build_collector
  mkdir -p "$OUTPUT_DIR"

  for version in "${REQUESTED_VERSIONS[@]}"; do
    assert_version_supported "$version"
    run_version_e2e "$version"
  done
}

main "$@"
