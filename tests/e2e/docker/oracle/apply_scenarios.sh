#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-seed}"
PROJECT_NAME="${2:?missing compose project name}"
BASE_COMPOSE_FILE="${3:?missing base compose file}"
OVERRIDE_COMPOSE_FILE="${4:?missing oracle override compose file}"
CONNECT_STRING="${5:?missing oracle connect string}"
SCHEMA_USER="${6:?missing oracle schema user}"
STATE_DIR="/tmp/dbcheck-oracle-e2e-${PROJECT_NAME}"

compose_exec() {
  docker compose -p "$PROJECT_NAME" -f "$BASE_COMPOSE_FILE" -f "$OVERRIDE_COMPOSE_FILE" "$@"
}

oracle_exec() {
  compose_exec exec -T -e DBCHECK_CONNECT_STRING="$CONNECT_STRING" oracle sh -lc '
set -eu
SQLPLUS_BIN=""
for candidate in \
  "${ORACLE_HOME:-}/bin/sqlplus" \
  /opt/oracle/product/19c/dbhome_1/bin/sqlplus \
  /u01/app/oracle/product/11.2.0/xe/bin/sqlplus
do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    SQLPLUS_BIN="$candidate"
    break
  fi
done
if [ -z "$SQLPLUS_BIN" ]; then
  echo "[ERROR] sqlplus binary not found inside oracle container" >&2
  exit 1
fi
if [ -z "${ORACLE_HOME:-}" ]; then
  ORACLE_HOME="$(cd "$(dirname "$SQLPLUS_BIN")/.." && pwd)"
  export ORACLE_HOME
fi
PATH="$ORACLE_HOME/bin:$PATH"
export PATH
exec "$SQLPLUS_BIN" -s "$DBCHECK_CONNECT_STRING"
'
}

run_sql() {
  local sql="$1"
  oracle_exec <<SQL
SET HEADING OFF
SET FEEDBACK OFF
SET PAGESIZE 0
SET VERIFY OFF
SET ECHO OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE
$sql
EXIT
SQL
}

wait_for_oracle() {
  local attempt
  for attempt in $(seq 1 120); do
    if run_sql "SELECT 1 FROM dual;" >/dev/null 2>&1; then
      echo "[INFO] oracle is ready after ${attempt} attempt(s)"
      return 0
    fi
    sleep 5
  done
  echo "[ERROR] oracle did not become ready in time" >&2
  return 1
}

seed_oracle() {
  wait_for_oracle
  run_sql "$(cat <<SQL
DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = UPPER('${SCHEMA_USER}');
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER ${SCHEMA_USER} IDENTIFIED BY dbcheckpwd DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP QUOTA UNLIMITED ON USERS';
    EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO ${SCHEMA_USER}';
  END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'CREATE TABLE ${SCHEMA_USER}.DBCHECK_E2E_CASE (id NUMBER, note VARCHAR2(64), CONSTRAINT DBCHECK_E2E_PK PRIMARY KEY (id))';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
BEGIN
  FOR i IN 1..200 LOOP
    BEGIN
      INSERT INTO ${SCHEMA_USER}.DBCHECK_E2E_CASE (id, note) VALUES (i, RPAD('x', 64, 'x'));
    EXCEPTION
      WHEN DUP_VAL_ON_INDEX THEN
        NULL;
    END;
  END LOOP;
  COMMIT;
END;
/
ALTER TABLE ${SCHEMA_USER}.DBCHECK_E2E_CASE DISABLE CONSTRAINT DBCHECK_E2E_PK;
BEGIN
  EXECUTE IMMEDIATE 'CREATE TABLE ${SCHEMA_USER}.DBCHECK_E2E_TRIGGER_CASE (id NUMBER PRIMARY KEY, note VARCHAR2(32))';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN
      RAISE;
    END IF;
END;
/
CREATE OR REPLACE TRIGGER ${SCHEMA_USER}.DBCHECK_E2E_TRG
BEFORE INSERT ON ${SCHEMA_USER}.DBCHECK_E2E_TRIGGER_CASE
FOR EACH ROW
BEGIN
  :NEW.note := UPPER(:NEW.note);
END;
/
ALTER TRIGGER ${SCHEMA_USER}.DBCHECK_E2E_TRG DISABLE;
CREATE OR REPLACE FORCE VIEW ${SCHEMA_USER}.DBCHECK_E2E_INVALID_VIEW AS
SELECT * FROM ${SCHEMA_USER}.DBCHECK_E2E_MISSING_TABLE;
DECLARE
  v_count NUMBER;
BEGIN
  FOR i IN 1..20 LOOP
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ${SCHEMA_USER}.DBCHECK_E2E_CASE WHERE note LIKE ''%x%''' INTO v_count;
  END LOOP;
END;
/
SELECT COUNT(*) FROM ${SCHEMA_USER}.DBCHECK_E2E_CASE;
SQL
)"
  mkdir -p "$STATE_DIR"
  echo "[INFO] oracle scenarios applied for ${SCHEMA_USER}"
}

main() {
  case "$MODE" in
    seed)
      seed_oracle
      ;;
    *)
      echo "[ERROR] unsupported mode: $MODE" >&2
      exit 1
      ;;
  esac
}

main "$@"
