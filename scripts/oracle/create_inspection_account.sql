-- Run as a DBA in the container to be inspected, with populated bind variables
-- :inspection_account and :inspection_password. No SQL*Plus text substitution.
SET DEFINE OFF
SET VERIFY OFF
SET ECHO OFF
SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT SQL.SQLCODE
-- Supplemental current-state fixed-view grants. No licensed diagnostic views.
DECLARE
  account_name VARCHAR2(128);
BEGIN
  IF :inspection_account IS NULL OR NOT REGEXP_LIKE(:inspection_account, '^[A-Za-z][A-Za-z0-9_$#]{0,29}$') THEN
    RAISE_APPLICATION_ERROR(-20001, 'Account must be a simple identifier of at most 30 characters.');
  END IF;
  IF :inspection_password IS NULL OR NOT REGEXP_LIKE(:inspection_password, '^[A-Za-z0-9_#$]{8,128}$') THEN
    RAISE_APPLICATION_ERROR(-20002, 'Password must be 8-128 ASCII letters, digits, underscore, hash or dollar.');
  END IF;
  account_name := DBMS_ASSERT.ENQUOTE_NAME(UPPER(:inspection_account), FALSE);
  EXECUTE IMMEDIATE 'CREATE USER ' || account_name || ' IDENTIFIED BY "' || :inspection_password || '"';
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION, SELECT_CATALOG_ROLE TO ' || account_name;
  FOR item IN (
    SELECT column_value AS object_name FROM TABLE(sys.odcivarchar2list(
      'V_$INSTANCE', 'V_$DATABASE', 'V_$PARAMETER', 'GV_$PARAMETER',
      'V_$DATAFILE', 'V_$TEMPFILE', 'V_$LOGFILE', 'V_$LOG', 'V_$CONTROLFILE',
      'V_$RECOVER_FILE', 'V_$RMAN_BACKUP_JOB_DETAILS', 'V_$ARCHIVE_DEST',
      'V_$ARCHIVE_DEST_STATUS', 'V_$ARCHIVED_LOG', 'V_$RECOVERY_FILE_DEST',
      'GV_$SESSION', 'GV_$TRANSACTION', 'GV_$SQL', 'V_$RESOURCE_LIMIT',
      'V_$LOG_HISTORY', 'V_$SYSSTAT', 'V_$SYSTEM_EVENT', 'V_$LATCH',
      'V_$SYS_TIME_MODEL', 'V_$UNDOSTAT', 'V_$SGA_RESIZE_OPS',
      'V_$SYSMETRIC', 'V_$FILESTAT', 'V_$SESSTAT', 'V_$STATNAME',
      'V_$TEMP_SPACE_HEADER', 'V_$SORT_SEGMENT', 'V_$ENCRYPTION_WALLET',
      'GV_$SORT_USAGE', 'DBA_TEMP_FILES', 'DBA_TABLESPACES',
      'DBA_SEGMENTS', 'DBA_RECYCLEBIN',
      'V_$DATABASE_BLOCK_CORRUPTION', 'V_$DATAGUARD_STATS', 'V_$ARCHIVE_GAP',
      'V_$ASM_DISKGROUP_STAT', 'GV_$INSTANCE',
      'V_$PDBS', 'V_$DIAG_ALERT_EXT', 'DBA_REGISTRY_SQLPATCH', 'V_$OPTION',
      'DBA_USERS_WITH_DEFPWD', 'DBA_PROFILES', 'DBA_SYS_PRIVS', 'DBA_DB_LINKS',
      'DBA_REGISTRY', 'DBA_REGISTRY_HISTORY'
    ))
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'GRANT SELECT ON SYS.' || item.object_name || ' TO ' || account_name;
    EXCEPTION WHEN OTHERS THEN
      IF SQLCODE = -942 THEN
        DBMS_OUTPUT.PUT_LINE('Unavailable on this version: SYS.' || item.object_name);
      ELSE RAISE;
      END IF;
    END;
  END LOOP;
END;
/
EXEC :inspection_password := NULL;
