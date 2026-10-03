package oracle

import (
	"context"
	"regexp"
	"strings"
)

var oracleCode = regexp.MustCompile(`ORA-\d{5}`)
var queryObjects = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+((?:sys\.)?(?:g?v\$|dba_|nls_)[a-z0-9_$]+)`)

func datasetPath(scope string) string {
	parts := strings.Split(scope, ".")
	if len(parts) < 2 || parts[0] != "oracle" {
		return scope
	}
	parts[0] = "db"
	aliases := map[string]string{"basic": "basic_info", "config": "config_check", "sql": "sql_analysis", "topology": "deployment_topology", "version": "version_info"}
	if alias, ok := aliases[parts[1]]; ok {
		parts[1] = alias
	}
	if len(parts) > 2 && parts[1] == "sql_analysis" {
		names := map[string]string{"top_elapsed": "top_sql_by_elapsed_time", "top_buffer_gets": "top_sql_by_buffer_gets", "top_disk_reads": "top_sql_by_disk_reads", "top_executions": "top_sql_by_executions"}
		if alias, ok := names[parts[2]]; ok {
			parts[2] = alias
		}
	}
	return strings.Join(parts, ".")
}

func (c *metricsCollector) markUnavailable(scope, reason, remediation string) {
	c.availability[datasetPath(scope)] = map[string]any{"readable": false, "reason": reason, "remediation": remediation}
}

func (c *metricsCollector) recordQuery(scope, query string, err error) {
	path := datasetPath(scope)
	objects := []string{}
	for _, match := range queryObjects.FindAllStringSubmatch(query, -1) {
		objects = append(objects, grantObject(match[1]))
	}
	item := map[string]any{"readable": err == nil, "required_objects": objects}
	if err != nil {
		item["error_code"] = oracleCode.FindString(err.Error())
		item["reason"] = err.Error()
		grants := []string{}
		for _, object := range objects {
			grants = append(grants, "GRANT SELECT ON "+object+" TO <inspection_account>;")
		}
		item["remediation"] = "请客户 DBA 在当前连接容器确认视图存在并执行所需只读授权：" + strings.Join(grants, " ") + " 授权后用原连接参数重新运行 Collector；其他 Oracle 错误需先按错误码排查。"
	}
	if previous, ok := c.availability[path].(map[string]any); ok && previous["readable"] == false && err == nil {
		previous["required_objects"] = objects
		if len(objects) > 0 {
			grants := []string{}
			for _, object := range objects {
				grants = append(grants, "GRANT SELECT ON "+object+" TO <inspection_account>;")
			}
			previous["remediation"] = "请客户 DBA 在当前连接容器确认视图存在，执行 " + strings.Join(grants, " ") + " 后用原连接参数重新运行 Collector。其他错误先按 Oracle 错误码排查。"
		}
		return
	}
	c.availability[path] = item
}

func grantObject(object string) string {
	object = strings.TrimPrefix(strings.ToUpper(object), "SYS.")
	if strings.HasPrefix(object, "GV$") {
		object = "GV_$" + object[3:]
	} else if strings.HasPrefix(object, "V$") {
		object = "V_$" + object[2:]
	}
	return "SYS." + object
}

// Probe actual reads because roles alone do not establish effective access.
func (c *metricsCollector) precheckPrivileges(ctx context.Context, major int) map[string]any {
	objects := []string{"V_$INSTANCE", "V_$DATABASE", "V_$PARAMETER", "V_$DATAFILE", "V_$TEMPFILE", "V_$LOGFILE", "V_$CONTROLFILE", "V_$LOG", "V_$RECOVER_FILE", "V_$RMAN_BACKUP_JOB_DETAILS", "V_$ARCHIVE_DEST", "V_$ARCHIVED_LOG", "V_$RECOVERY_FILE_DEST", "GV_$SESSION", "GV_$TRANSACTION", "GV_$SQL", "V_$RESOURCE_LIMIT", "V_$LOG_HISTORY", "V_$SYSSTAT", "V_$SYSTEM_EVENT", "V_$LATCH", "V_$SYS_TIME_MODEL", "V_$UNDOSTAT", "V_$SGA_RESIZE_OPS", "DBA_TABLESPACES", "DBA_DATA_FILES", "DBA_FREE_SPACE", "DBA_OBJECTS", "DBA_INDEXES", "DBA_TABLES", "DBA_USERS", "DBA_ROLE_PRIVS", "DBA_CONSTRAINTS", "DBA_TRIGGERS"}
	objects = append(objects, "DBA_USERS_WITH_DEFPWD", "DBA_PROFILES", "DBA_SYS_PRIVS", "DBA_DB_LINKS", "DBA_REGISTRY", "V_$OPTION", "V_$ENCRYPTION_WALLET", "V_$DATABASE_BLOCK_CORRUPTION")
	if major == 11 {
		objects = append(objects, "DBA_REGISTRY_HISTORY")
	}
	if major >= 12 {
		objects = append(objects, "V_$PDBS", "V_$DIAG_ALERT_EXT", "DBA_REGISTRY_SQLPATCH")
	}
	objects = append(objects, "DBA_TEMP_FILES", "DBA_SEGMENTS", "DBA_RECYCLEBIN", "GV_$TEMPSEG_USAGE")
	probes := []map[string]any{}
	for _, object := range objects {
		rows, err := c.db.QueryContext(ctx, "SELECT * FROM SYS."+object+" WHERE 1=0")
		if err == nil {
			err = rows.Err()
			rows.Close()
		}
		probe := map[string]any{"object": "SYS." + object, "readable": err == nil}
		if err != nil {
			probe["error_code"] = oracleCode.FindString(err.Error())
			probe["reason"] = err.Error()
			probe["remediation"] = "请 DBA 在当前连接容器执行 GRANT SELECT ON SYS." + object + " TO <inspection_account>; 后重新采集。ORA-00942 时先确认此版本存在该视图。"
		}
		probes = append(probes, probe)
	}
	return map[string]any{"account": c.cfg.DBUsername, "object_probes": probes}
}
