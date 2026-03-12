#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
MYSQL_BASE_COMPOSE_FILE="$ROOT_DIR/tests/e2e/docker/docker-compose.yml"
ORACLE_BASE_COMPOSE_FILE="$ROOT_DIR/tests/e2e/docker/docker-compose.oracle-base.yml"
MYSQL_SCENARIO_SCRIPT="$ROOT_DIR/tests/e2e/docker/mysql/apply_scenarios.sh"
ORACLE_SCENARIO_SCRIPT="$ROOT_DIR/tests/e2e/docker/oracle/apply_scenarios.sh"
RUNS_ROOT="$ROOT_DIR/tests/e2e/runs"
BUILD_BIN_DIR="${BUILD_BIN_DIR:-$ROOT_DIR/tmp/e2e-bin}"
TIMESTAMP="$(date +%Y%m%dT%H%M%S)"
OUTPUT_DIR="$RUNS_ROOT/$TIMESTAMP"
DB_TYPE="mysql"
MYSQL_VERSIONS=("5.6" "5.7" "8.0")
ORACLE_VERSIONS=("11g" "19c")
REQUESTED_VERSIONS=()
OS_TARGET_HOST="127.0.0.1"
OS_TARGET_PORT="12222"
OS_TARGET_USER="root"
OS_TARGET_PASSWORD="rootpwd"

assert_commands() {
  command -v docker >/dev/null
  docker compose version >/dev/null
  if ! docker info >/dev/null 2>&1; then
    echo "[ERROR] docker daemon is not accessible in current environment" >&2
    exit 1
  fi
  command -v go >/dev/null
  command -v python3 >/dev/null
  test -x "$MYSQL_SCENARIO_SCRIPT"
  test -x "$ORACLE_SCENARIO_SCRIPT"
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
      --db-type)
        DB_TYPE="${2:?missing db type}"
        shift 2
        ;;
      --mysql-version)
        REQUESTED_VERSIONS+=("${2:?missing mysql version}")
        shift 2
        ;;
      --oracle-version)
        REQUESTED_VERSIONS+=("${2:?missing oracle version}")
        shift 2
        ;;
      *)
        echo "[ERROR] unsupported argument: $1" >&2
        exit 1
        ;;
    esac
  done
  if [[ "$DB_TYPE" != "mysql" && "$DB_TYPE" != "oracle" ]]; then
    echo "[ERROR] unsupported db type: $DB_TYPE" >&2
    exit 1
  fi
  if ((${#REQUESTED_VERSIONS[@]} == 0)); then
    if [[ "$DB_TYPE" == "mysql" ]]; then
      REQUESTED_VERSIONS=("${MYSQL_VERSIONS[@]}")
    else
      REQUESTED_VERSIONS=("${ORACLE_VERSIONS[@]}")
    fi
  fi
}

assert_version_supported() {
  local version="$1"
  local supported_versions=()
  local supported
  if [[ "$DB_TYPE" == "mysql" ]]; then
    supported_versions=("${MYSQL_VERSIONS[@]}")
  else
    supported_versions=("${ORACLE_VERSIONS[@]}")
  fi
  for supported in "${supported_versions[@]}"; do
    if [[ "$supported" == "$version" ]]; then
      return 0
    fi
  done
  echo "[ERROR] unsupported ${DB_TYPE} version: $version" >&2
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

oracle_override_file() {
  case "$1" in
    11g) printf '%s' "$ROOT_DIR/tests/e2e/docker/docker-compose.oracle11g.yml" ;;
    19c) printf '%s' "$ROOT_DIR/tests/e2e/docker/docker-compose.oracle19c.yml" ;;
    *) return 1 ;;
  esac
}

oracle_host_port() {
  case "$1" in
    11g) printf '%s' '11521' ;;
    19c) printf '%s' '11919' ;;
    *) return 1 ;;
  esac
}

oracle_sid() {
  case "$1" in
    11g) printf '%s' 'XE' ;;
    19c) printf '%s' 'ORCLCDB' ;;
    *) return 1 ;;
  esac
}

oracle_password() {
  case "$1" in
    11g) printf '%s' 'oracle' ;;
    19c) printf '%s' 'oraclepwd' ;;
    *) return 1 ;;
  esac
}

oracle_connect_string() {
  case "$1" in
    11g) printf '%s' 'system/oracle@//localhost:1521/XE' ;;
    19c) printf '%s' 'system/oraclepwd@//localhost:1521/ORCLCDB' ;;
    *) return 1 ;;
  esac
}

oracle_schema_user() {
  case "$1" in
    11g) printf '%s' 'DBCHECK' ;;
    19c) printf '%s' 'C##DBCHECK' ;;
    *) return 1 ;;
  esac
}

version_slug() {
  printf '%s' "${1//./}"
}

cleanup_compose() {
  local project="$1"
  local base="$2"
  local override="$3"
  docker compose -p "$project" -f "$base" -f "$override" down -v --remove-orphans >/dev/null 2>&1 || true
}

stop_runtime_scenarios() {
  local project="$1"
  local base="$2"
  local override="$3"
  "$MYSQL_SCENARIO_SCRIPT" stop-runtime "$project" "$override" >/dev/null 2>&1 || true
  cleanup_compose "$project" "$base" "$override"
}

run_mysql_collector() {
  local db_port="$1"
  local output_dir="$2"
  "$BUILD_BIN_DIR/db-collector" \
    --db-type mysql \
    --db-host 127.0.0.1 \
    --db-port "$db_port" \
    --db-username root \
    --db-password rootpwd \
    --dbname dbcheck \
    --os-host "$OS_TARGET_HOST" \
    --os-port "$OS_TARGET_PORT" \
    --os-username "$OS_TARGET_USER" \
    --os-password "$OS_TARGET_PASSWORD" \
    --output-dir "$output_dir"
}

run_oracle_collector() {
  local version="$1"
  local db_port="$2"
  local output_dir="$3"
  "$BUILD_BIN_DIR/db-collector" \
    --db-type oracle \
    --db-host 127.0.0.1 \
    --db-port "$db_port" \
    --db-username system \
    --db-password "$(oracle_password "$version")" \
    --dbname "$(oracle_sid "$version")" \
    --os-host "$OS_TARGET_HOST" \
    --os-port "$OS_TARGET_PORT" \
    --os-username "$OS_TARGET_USER" \
    --os-password "$OS_TARGET_PASSWORD" \
    --output-dir "$output_dir"
}

build_binaries() {
  echo "[INFO] building embedded os probes"
  "$ROOT_DIR/scripts/build_embedded_osprobes.sh"
  echo "[INFO] building db-collector"
  mkdir -p "$BUILD_BIN_DIR"
  GOCACHE=/tmp/go-cache go build -o "$BUILD_BIN_DIR/db-collector" "$ROOT_DIR/collector/cmd/db-collector"
  echo "[INFO] building db-reporter"
  GOCACHE=/tmp/go-cache go build -o "$BUILD_BIN_DIR/db-reporter" "$ROOT_DIR/reporter/cmd/db-reporter"
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

run_mysql_e2e() (
  set -euo pipefail
  local version="$1"
  local override db_port project version_output_dir collector_output run_id run_dir report_md report
  local runtime_started=0

  cleanup_version() {
    if [[ "$runtime_started" == "1" ]]; then
      "$MYSQL_SCENARIO_SCRIPT" stop-runtime "$project" "$override" >/dev/null 2>&1 || true
    fi
    cleanup_compose "$project" "$MYSQL_BASE_COMPOSE_FILE" "$override"
  }

  override="$(mysql_override_file "$version")"
  db_port="$(mysql_host_port "$version")"
  project="dbcheck-e2e-mysql-$(version_slug "$version")"
  version_output_dir="$OUTPUT_DIR/mysql-$version"
  mkdir -p "$version_output_dir"

  cleanup_compose "$project" "$MYSQL_BASE_COMPOSE_FILE" "$override"
  trap cleanup_version EXIT

  echo "[INFO] starting docker services for MySQL $version"
  docker compose -p "$project" -f "$MYSQL_BASE_COMPOSE_FILE" -f "$override" up -d --wait

  echo "[INFO] applying mysql scenarios for MySQL $version"
  "$MYSQL_SCENARIO_SCRIPT" seed "$project" "$override"

  echo "[INFO] starting runtime contention for MySQL $version"
  "$MYSQL_SCENARIO_SCRIPT" start-runtime "$project" "$override"
  runtime_started=1

  echo "[INFO] running collector for MySQL $version"
  collector_output="$(GOCACHE=/tmp/go-cache run_mysql_collector "$db_port" "$version_output_dir")"
  printf '%s\n' "$collector_output"
  "$MYSQL_SCENARIO_SCRIPT" stop-runtime "$project" "$override"
  runtime_started=0

  run_id="$(extract_run_id "$collector_output")"
  run_dir="$version_output_dir/$run_id"
  report_md="$run_dir/report.md"
  report="$run_dir/report.docx"

  echo "[INFO] running db-reporter for MySQL $version"
  "$BUILD_BIN_DIR/db-reporter" \
    --run-dir "$run_dir" \
    --out-md "$report_md" \
    --out-docx "$report" \
    --document-name "$(basename "$report")" \
    --inspector "db-check" \
    --mysql-version "$version"

  echo "[INFO] docker e2e succeeded for MySQL $version"
  echo "[INFO] artifacts[$version]: $run_dir"
)

run_oracle_e2e() (
  set -euo pipefail
  local version="$1"
  local override db_port project version_output_dir collector_output run_id run_dir report_md report

  cleanup_version() {
    cleanup_compose "$project" "$ORACLE_BASE_COMPOSE_FILE" "$override"
  }

  override="$(oracle_override_file "$version")"
  db_port="$(oracle_host_port "$version")"
  project="dbcheck-e2e-oracle-$(version_slug "$version")"
  version_output_dir="$OUTPUT_DIR/oracle-$version"
  mkdir -p "$version_output_dir"

  cleanup_compose "$project" "$ORACLE_BASE_COMPOSE_FILE" "$override"
  trap cleanup_version EXIT

  echo "[INFO] starting docker services for Oracle $version"
  docker compose -p "$project" -f "$ORACLE_BASE_COMPOSE_FILE" -f "$override" up -d

  echo "[INFO] applying oracle scenarios for Oracle $version"
  "$ORACLE_SCENARIO_SCRIPT" seed "$project" "$ORACLE_BASE_COMPOSE_FILE" "$override" "$(oracle_connect_string "$version")" "$(oracle_schema_user "$version")"

  echo "[INFO] running collector for Oracle $version"
  collector_output="$(GOCACHE=/tmp/go-cache run_oracle_collector "$version" "$db_port" "$version_output_dir")"
  printf '%s\n' "$collector_output"

  run_id="$(extract_run_id "$collector_output")"
  run_dir="$version_output_dir/$run_id"
  report_md="$run_dir/report.md"
  report="$run_dir/report.docx"

  echo "[INFO] running db-reporter for Oracle $version"
  "$BUILD_BIN_DIR/db-reporter" \
    --run-dir "$run_dir" \
    --rule-file "$ROOT_DIR/rules/oracle/rule.json" \
    --template-file "$ROOT_DIR/reporter/templates/mysql-template.docx" \
    --out-md "$report_md" \
    --out-docx "$report" \
    --document-name "$(basename "$report")" \
    --inspector "db-check" \
    --change-description "oracle巡检报告"

  echo "[INFO] docker e2e succeeded for Oracle $version"
  echo "[INFO] artifacts[$version]: $run_dir"
)

main() {
  local version
  parse_args "$@"
  assert_commands
  assert_venv
  assert_python_deps
  build_binaries
  mkdir -p "$OUTPUT_DIR"

  for version in "${REQUESTED_VERSIONS[@]}"; do
    assert_version_supported "$version"
    if [[ "$DB_TYPE" == "mysql" ]]; then
      run_mysql_e2e "$version"
    else
      run_oracle_e2e "$version"
    fi
  done
}

main "$@"
