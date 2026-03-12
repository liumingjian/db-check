package oracle

import "context"

func (c *metricsCollector) collectSecurity(ctx context.Context) map[string]any {
	return map[string]any{
		"disabled_constraints": rowsPayload(c.queryRows(ctx, "oracle.security.disabled_constraints", disabledConstraintsQuery)),
		"disabled_triggers":    rowsPayload(c.queryRows(ctx, "oracle.security.disabled_triggers", disabledTriggersQuery)),
		"expired_users": rowsPayload(c.queryRows(
			ctx,
			"oracle.security.expired_users",
			`SELECT username AS "username", account_status AS "account_status", expiry_date AS "expiry_date" FROM dba_users WHERE account_status LIKE 'EXPIRED%'`,
		)),
		"table_degree_gt_one": rowsPayload(c.queryRows(
			ctx,
			"oracle.security.table_degree_gt_one",
			`SELECT table_name AS "table_name", degree AS "degree" FROM dba_tables WHERE degree > '1'`,
		)),
		"indexes_degree_gt_one": rowsPayload(c.queryRows(
			ctx,
			"oracle.security.indexes_degree_gt_one",
			`SELECT index_name AS "index_name", degree AS "degree" FROM dba_indexes WHERE degree > '1'`,
		)),
		"dba_role_users": rowsPayload(c.queryRows(
			ctx,
			"oracle.security.dba_role_users",
			`SELECT grantee AS "grantee", granted_role AS "granted_role", admin_option AS "admin_option", default_role AS "default_role" FROM dba_role_privs WHERE granted_role='DBA' OR granted_role='SYSDBA'`,
		)),
	}
}

const disabledConstraintsQuery = `
SELECT owner AS "owner",
       constraint_name AS "constraint_name",
       constraint_type AS "constraint_type",
       table_name AS "table_name",
       status AS "status"
  FROM dba_constraints
 WHERE status = 'DISABLED'
   AND owner NOT IN (
     SELECT username FROM dba_users WHERE default_tablespace IN ('SYSTEM', 'SYSAUX')
   )`

const disabledTriggersQuery = `
SELECT owner AS "owner",
       trigger_name AS "trigger_name",
       trigger_type AS "trigger_type",
       triggering_event AS "triggering_event",
       table_name AS "table_name",
       status AS "status"
  FROM dba_triggers
 WHERE status = 'DISABLED'
   AND owner NOT IN (
     SELECT username FROM dba_users WHERE default_tablespace IN ('SYSTEM', 'SYSAUX')
   )`
