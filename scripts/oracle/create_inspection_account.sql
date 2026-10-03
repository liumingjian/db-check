-- Run as a DBA in the container to be inspected. SQL*Plus hides the password.
SET VERIFY OFF
SET ECHO OFF
SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT SQL.SQLCODE
ACCEPT inspection_account CHAR PROMPT 'Inspection account (simple Oracle identifier): '
ACCEPT inspection_password CHAR PROMPT 'New password (no double quotes): ' HIDE
CREATE USER &inspection_account IDENTIFIED BY "&inspection_password";
UNDEFINE inspection_password
GRANT CREATE SESSION, SELECT_CATALOG_ROLE TO &inspection_account;

-- Supplemental current-state fixed-view grants. No licensed diagnostic views.
BEGIN
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
      'GV_$TEMPSEG_USAGE', 'DBA_TEMP_FILES', 'DBA_TABLESPACES',
      'DBA_SEGMENTS', 'DBA_RECYCLEBIN',
      'V_$DATABASE_BLOCK_CORRUPTION', 'V_$DATAGUARD_STATS', 'V_$ARCHIVE_GAP',
      'V_$ASM_DISKGROUP_STAT', 'GV_$INSTANCE',
      'V_$PDBS', 'V_$DIAG_ALERT_EXT', 'DBA_REGISTRY_SQLPATCH', 'V_$OPTION',
      'DBA_USERS_WITH_DEFPWD', 'DBA_PROFILES', 'DBA_SYS_PRIVS', 'DBA_DB_LINKS',
      'DBA_REGISTRY', 'DBA_REGISTRY_HISTORY'
    ))
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'GRANT SELECT ON SYS.' || item.object_name || ' TO &inspection_account';
    EXCEPTION WHEN OTHERS THEN
      IF SQLCODE = -942 THEN
        DBMS_OUTPUT.PUT_LINE('Unavailable on this version: SYS.' || item.object_name);
      ELSE RAISE;
      END IF;
    END;
  END LOOP;
END;
/
UNDEFINE inspection_account
