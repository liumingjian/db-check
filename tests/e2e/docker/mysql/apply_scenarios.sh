#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../../.." && pwd)"
BASE_COMPOSE_FILE="$ROOT_DIR/tests/e2e/docker/docker-compose.yml"
SCENARIO_SQL="$ROOT_DIR/tests/e2e/docker/mysql/scenarios.sql"
MODE="${1:-seed}"
PROJECT_NAME="${2:?missing compose project name}"
OVERRIDE_COMPOSE_FILE="${3:?missing mysql override compose file}"
STATE_DIR="/tmp/dbcheck-e2e-${PROJECT_NAME}"

compose_exec() {
  docker compose -p "$PROJECT_NAME" -f "$BASE_COMPOSE_FILE" -f "$OVERRIDE_COMPOSE_FILE" "$@"
}

run_root_sql() {
  local sql="$1"
  compose_exec exec -T mysql mysql -uroot -prootpwd dbcheck -e "$sql"
}

apply_scenario_sql_file() {
  compose_exec exec -T mysql mysql -uroot -prootpwd dbcheck < "$SCENARIO_SQL"
}

inject_lock_wait_timeout() {
  compose_exec exec -T mysql sh -c \
    "mysql -uroot -prootpwd dbcheck -e \"START TRANSACTION; UPDATE lock_wait_case SET note='holder' WHERE id=1; SELECT SLEEP(6); COMMIT;\"" &
  local holder_pid=$!
  sleep 1
  if compose_exec exec -T mysql sh -c \
    "mysql -uroot -prootpwd dbcheck -e \"SET SESSION innodb_lock_wait_timeout=2; START TRANSACTION; UPDATE lock_wait_case SET note='waiter' WHERE id=1; COMMIT;\""; then
    echo "[WARN] lock wait scenario did not timeout as expected"
  else
    echo "[INFO] lock wait timeout injected as expected"
  fi
  if ! wait "$holder_pid"; then
    echo "[WARN] lock holder session exited with non-zero status"
  fi
}

inject_auth_failures() {
  local idx
  for idx in 1 2 3; do
    if compose_exec exec -T mysql mysql -uchecker -pwrong-password dbcheck -e "SELECT 1" >/dev/null 2>&1; then
      echo "[WARN] auth failure scenario did not fail on attempt $idx"
    else
      echo "[INFO] auth failure injected #$idx"
    fi
  done
}

start_mysql_session() {
  local name="$1"
  local sql="$2"
  mkdir -p "$STATE_DIR"
  local output_file="$STATE_DIR/${name}.out"
  printf '%s\n' "$sql" | compose_exec exec -T mysql mysql -N -uroot -prootpwd dbcheck >"$output_file" 2>&1 &
  echo "$!" > "$STATE_DIR/${name}.host_pid"
}

start_runtime_contention() {
  rm -rf "$STATE_DIR"
  mkdir -p "$STATE_DIR"

  start_mysql_session "row_holder" "$(cat <<'SQL'
SET SESSION innodb_lock_wait_timeout=30;
START TRANSACTION;
UPDATE lock_wait_case SET note='holder-runtime' WHERE id=1;
SELECT SLEEP(30);
ROLLBACK;
SQL
)"
  sleep 1

  start_mysql_session "row_waiter" "$(cat <<'SQL'
SET SESSION innodb_lock_wait_timeout=30;
START TRANSACTION;
UPDATE lock_wait_case SET note='waiter-runtime' WHERE id=1;
ROLLBACK;
SQL
)"
  sleep 1

  start_mysql_session "mdl_holder" "$(cat <<'SQL'
START TRANSACTION;
SELECT * FROM ddl_lock_case WHERE id=1 LOCK IN SHARE MODE;
SELECT SLEEP(30);
ROLLBACK;
SQL
)"
  sleep 1

  start_mysql_session "mdl_waiter" "$(cat <<'SQL'
ALTER TABLE ddl_lock_case ENGINE=InnoDB;
SQL
)"
  sleep 1
  report_runtime_counts
  echo "[INFO] runtime contention started"
}

kill_runtime_session() {
  local name="$1"
  local host_pid_file="$STATE_DIR/${name}.host_pid"
  if [[ -f "$host_pid_file" ]]; then
    local host_pid
    host_pid="$(cat "$host_pid_file")"
    kill "$host_pid" >/dev/null 2>&1 || true
    wait "$host_pid" >/dev/null 2>&1 || true
  fi
}

report_runtime_counts() {
  local row_waits
  row_waits="$(run_root_sql "SELECT COUNT(*) AS cnt FROM information_schema.innodb_trx WHERE trx_state='LOCK WAIT';" | awk 'END{print $NF}')"
  echo "[INFO] runtime row lock waits: ${row_waits:-0}"
  if compose_exec exec -T mysql mysql -uroot -prootpwd dbcheck -e "SELECT COUNT(*) AS cnt FROM performance_schema.metadata_locks WHERE LOCK_STATUS='PENDING';" >/tmp/dbcheck-e2e-mdl-"$PROJECT_NAME".out 2>/dev/null; then
    local mdl_waits
    mdl_waits="$(awk 'END{print $NF}' /tmp/dbcheck-e2e-mdl-"$PROJECT_NAME".out)"
    echo "[INFO] runtime metadata lock waits: ${mdl_waits:-0}"
    rm -f /tmp/dbcheck-e2e-mdl-"$PROJECT_NAME".out
  fi
}

stop_runtime_contention() {
  if [[ ! -d "$STATE_DIR" ]]; then
    return 0
  fi
  kill_runtime_session "mdl_waiter"
  kill_runtime_session "mdl_holder"
  kill_runtime_session "row_waiter"
  kill_runtime_session "row_holder"
  rm -rf "$STATE_DIR"
  echo "[INFO] runtime contention stopped"
}

seed_scenarios() {
  echo "[INFO] applying mysql scenario sql"
  apply_scenario_sql_file

  echo "[INFO] enforcing slow query switches"
  run_root_sql "SET GLOBAL slow_query_log='ON'; SET GLOBAL long_query_time=0.05; SET GLOBAL log_output='TABLE';"

  echo "[INFO] generating extra slow queries"
  run_root_sql "SELECT SLEEP(0.2);"
  run_root_sql "SELECT SLEEP(0.22);"

  echo "[INFO] generating lock wait scenario"
  inject_lock_wait_timeout

  echo "[INFO] generating auth failure scenario"
  inject_auth_failures

  echo "[INFO] mysql scenarios applied"
}

main() {
  case "$MODE" in
    seed)
      seed_scenarios
      ;;
    start-runtime)
      start_runtime_contention
      ;;
    stop-runtime)
      stop_runtime_contention
      ;;
    *)
      echo "[ERROR] unsupported mode: $MODE" >&2
      exit 1
      ;;
  esac
}

main "$@"
