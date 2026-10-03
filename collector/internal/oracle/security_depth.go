package oracle

import (
	"context"
	"strconv"
	"strings"
)

func (c *metricsCollector) collectSecurityDepth(ctx context.Context) map[string]any {
	payload := map[string]any{}
	queries := map[string]string{
		"default_password_users":      `SELECT username AS "username" FROM dba_users_with_defpwd ORDER BY username`,
		"weak_password_profiles":      weakPasswordProfilesQuery,
		"unlimited_resource_profiles": `SELECT p.profile AS "profile", p.resource_name AS "resource_name", CASE WHEN p.limit='DEFAULT' THEN d.limit ELSE p.limit END AS "effective_limit" FROM dba_profiles p LEFT JOIN dba_profiles d ON d.profile='DEFAULT' AND d.resource_name=p.resource_name AND d.resource_type=p.resource_type WHERE p.resource_type='KERNEL' AND p.resource_name IN ('SESSIONS_PER_USER','CPU_PER_SESSION','CONNECT_TIME','IDLE_TIME') AND CASE WHEN p.limit='DEFAULT' THEN d.limit ELSE p.limit END='UNLIMITED' AND EXISTS (SELECT 1 FROM dba_users u WHERE u.profile=p.profile AND u.account_status='OPEN') ORDER BY p.profile, p.resource_name`,
		"public_system_privileges":    `SELECT privilege AS "privilege", admin_option AS "admin_option" FROM dba_sys_privs WHERE grantee='PUBLIC' AND (privilege LIKE '%ANY%' OR privilege IN ('ALTER SYSTEM','ALTER DATABASE','CREATE USER','ALTER USER','DROP USER','BECOME USER','UNLIMITED TABLESPACE')) ORDER BY privilege`,
		"database_links":              `SELECT owner AS "owner", db_link AS "db_link", username AS "username", created AS "created" FROM dba_db_links WHERE username IS NOT NULL ORDER BY owner, db_link`,
		"encryption_wallets":          `SELECT wrl_type AS "wallet_type", status AS "status" FROM v$encryption_wallet`,
	}
	for name, query := range queries {
		payload[name] = rowsPayload(c.queryRows(ctx, "oracle.security."+name, query))
	}
	payload["audit_trail"] = c.queryString(ctx, "oracle.security.audit_trail", `SELECT value FROM v$parameter WHERE name='audit_trail'`)
	payload["resource_limit"] = c.queryString(ctx, "oracle.security.resource_limit", `SELECT value FROM v$parameter WHERE name='resource_limit'`)
	payload["encrypted_tablespaces"] = c.queryInt64(ctx, "oracle.security.encrypted_tablespaces", `SELECT COUNT(*) FROM dba_tablespaces WHERE encrypted='YES'`)
	// Pure unified auditing can be enabled while AUDIT_TRAIL is NONE.
	if strings.EqualFold(payload["audit_trail"].(string), "NONE") {
		unified := c.queryString(ctx, "oracle.security.unified_auditing", `SELECT NVL(MAX(value),'FALSE') FROM v$option WHERE parameter='Unified Auditing'`)
		if unified == "TRUE" {
			c.markUnavailable("oracle.security.audit_trail", "纯统一审计模式，AUDIT_TRAIL 不能判断审计策略是否启用", "请 DBA 核查 AUDIT_UNIFIED_ENABLED_POLICIES，并确认需要审计的操作已启用策略。")
		} else if gap, ok := c.availability["db.security.unified_auditing"].(map[string]any); ok && gap["readable"] == false {
			c.availability["db.security.audit_trail"] = gap
		}
	}
	return mergeMaps(payload, c.collectPatchRegistry(ctx))
}

const weakPasswordProfilesQuery = `
SELECT p.profile AS "profile", p.resource_name AS "resource_name",
       p.limit AS "configured_limit",
       CASE WHEN p.limit='DEFAULT' THEN d.limit ELSE p.limit END AS "effective_limit"
  FROM dba_profiles p
  LEFT JOIN dba_profiles d ON d.profile='DEFAULT' AND d.resource_name=p.resource_name AND d.resource_type=p.resource_type
 WHERE p.resource_type='PASSWORD'
   AND EXISTS (SELECT 1 FROM dba_users u WHERE u.profile=p.profile AND u.account_status='OPEN')
   AND ((p.resource_name='PASSWORD_VERIFY_FUNCTION' AND NVL(CASE WHEN p.limit='DEFAULT' THEN d.limit ELSE p.limit END,'NULL') IN ('NULL','UNLIMITED'))
     OR (p.resource_name IN ('FAILED_LOGIN_ATTEMPTS','PASSWORD_LIFE_TIME','PASSWORD_LOCK_TIME') AND CASE WHEN p.limit='DEFAULT' THEN d.limit ELSE p.limit END IN ('UNLIMITED','0')))
 ORDER BY p.profile, p.resource_name`

func (c *metricsCollector) collectPatchRegistry(ctx context.Context) map[string]any {
	payload := map[string]any{
		"invalid_components": rowsPayload(c.queryRows(ctx, "oracle.security.invalid_components", `SELECT comp_id AS "comp_id", comp_name AS "comp_name", version AS "version", status AS "status" FROM dba_registry WHERE status <> 'VALID' OR status IS NULL`)),
	}
	raw := c.queryString(ctx, "oracle.security.patch_version", `SELECT version FROM v$instance`)
	major, _ := strconv.Atoi(strings.SplitN(raw, ".", 2)[0])
	if major >= 12 {
		payload["patch_history"] = rowsPayload(c.queryRows(ctx, "oracle.security.patch_history", `SELECT patch_id AS "patch_id", patch_uid AS "patch_uid", action AS "action", status AS "status", action_time AS "action_time", description AS "description" FROM dba_registry_sqlpatch ORDER BY action_time DESC, patch_id`))
		payload["installed_sql_patches"] = rowsPayload(c.queryRows(ctx, "oracle.security.installed_sql_patches", `SELECT patch_id AS "patch_id", patch_uid AS "patch_uid", action_time AS "action_time", description AS "description" FROM (SELECT p.*, ROW_NUMBER() OVER (PARTITION BY patch_id, patch_uid ORDER BY action_time DESC, action DESC) rn FROM dba_registry_sqlpatch p WHERE status='SUCCESS') WHERE rn=1 AND action='APPLY'`))
		payload["failed_patch_attempts"] = rowsPayload(c.queryRows(ctx, "oracle.security.failed_patch_attempts", `SELECT patch_id AS "patch_id", action AS "action", status AS "status", action_time AS "action_time", description AS "description" FROM (SELECT p.*, ROW_NUMBER() OVER (PARTITION BY patch_id, patch_uid ORDER BY action_time DESC, action DESC) rn FROM dba_registry_sqlpatch p) WHERE rn=1 AND status <> 'SUCCESS'`))
	} else {
		if major == 11 {
			payload["patch_history"] = rowsPayload(c.queryRows(ctx, "oracle.security.patch_history", `SELECT action_time AS "action_time", action AS "action", namespace AS "namespace", version AS "version", id AS "patch_id", comments AS "description" FROM dba_registry_history ORDER BY action_time DESC`))
		}
		for _, name := range []string{"installed_sql_patches", "failed_patch_attempts"} {
			c.markUnavailable("oracle.security."+name, "版本无法从 SQL 补丁视图判断当前安装或失败状态", "请 DBA 在数据库主机运行 opatch lsinventory，提供 Oracle Home 补丁清单及补丁执行日志；11g 历史记录没有执行状态，不能证明当前安装。")
		}
		if major == 0 {
			c.markUnavailable("oracle.security.patch_history", "无法识别 Oracle 版本", "请 DBA 授予 SYS.V_$INSTANCE 读取权限后重新采集。")
			if gap, ok := c.availability["db.security.patch_version"].(map[string]any); ok && gap["readable"] == false {
				for _, name := range []string{"patch_history", "installed_sql_patches", "failed_patch_attempts"} {
					c.availability["db.security."+name] = gap
				}
			}
		}
	}
	return payload
}
