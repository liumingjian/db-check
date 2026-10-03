package oracle

import (
	"context"
	"strconv"
	"strings"
)

// Probes use unlicensed current-state views and stay in the connected container.
func (c *metricsCollector) collectDeployment(ctx context.Context) (map[string]any, map[string]any) {
	version := c.collectVersion(ctx)
	major := version["major"].(int)
	c.inspectionAccount = c.precheckPrivileges(ctx, major)
	role := c.queryString(ctx, "oracle.topology.database_role", `SELECT database_role FROM v$database`)
	normalizedRole := "unknown"
	if role == "PRIMARY" {
		normalizedRole = "primary"
	} else if strings.Contains(role, "STANDBY") {
		normalizedRole = "standby"
	}
	topology := map[string]any{
		"is_rac":        probeBoolean(c.queryString(ctx, "oracle.topology.is_rac", `SELECT value FROM v$parameter WHERE name='cluster_database'`)),
		"database_role": role, "role": normalizedRole, "is_cdb": nil,
		"is_asm":              probeBoolean(c.queryString(ctx, "oracle.topology.is_asm", `SELECT CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM (SELECT name FROM v$datafile UNION ALL SELECT name FROM v$tempfile UNION ALL SELECT member AS name FROM v$logfile) WHERE name LIKE '+%'`)),
		"connected_container": "", "inspection_scope": "connected_container",
		"pdbs": rowsPayload([]map[string]any{}), "pdb_list_state": "not_applicable",
	}
	c.collectContainerTopology(ctx, major, topology)
	if topology["pdb_list_state"] == "not_collected" {
		path := "db.deployment_topology.pdbs"
		if previous, ok := c.availability[path].(map[string]any); !ok || previous["readable"] != false {
			c.markUnavailable("oracle.topology.pdbs", "PDB 清单不完整或部署形态未知", topology["pdb_list_remediation"].(string))
		}
	}
	return version, topology
}

func (c *metricsCollector) collectVersion(ctx context.Context) map[string]any {
	raw := c.queryString(ctx, "oracle.version", `SELECT version FROM v$instance`)
	major, _ := strconv.Atoi(strings.SplitN(raw, ".", 2)[0])
	family := "unknown"
	switch major {
	case 11:
		if strings.HasPrefix(raw, "11.2.") {
			family = "11gR2"
		}
	case 12, 18:
		family = "12c"
	case 19:
		family = "19c"
	case 21:
		family = "21c"
	case 23:
		family = "23ai"
	}
	multitenant := major >= 12
	return map[string]any{
		"version": raw, "major": major, "family": family,
		"capabilities": map[string]any{
			"multitenant":        multitenant,
			"diagnostic_alert":   multitenant,
			"sql_patch_registry": multitenant,
		},
	}
}

func (c *metricsCollector) collectContainerTopology(ctx context.Context, major int, topology map[string]any) {
	if major == 11 {
		topology["is_cdb"] = false
	} else if major >= 12 {
		topology["is_cdb"] = probeBoolean(c.queryString(ctx, "oracle.topology.is_cdb", `SELECT cdb FROM v$database`))
		topology["connected_container"] = c.queryString(ctx, "oracle.topology.connected_container", `SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM dual`)
		if topology["is_cdb"] == nil {
			topology["pdb_list_state"] = "not_collected"
			topology["pdb_list_remediation"] = "Ask the DBA to grant SELECT on SYS.V_$DATABASE and rerun collection to detect whether the database is a CDB."
		}
		if topology["is_cdb"] == true {
			before := len(c.errors)
			topology["pdbs"] = rowsPayload(c.queryRows(ctx, "oracle.topology.pdbs", `SELECT con_id AS "con_id", name AS "name", open_mode AS "open_mode", restricted AS "restricted", total_size AS "size_bytes" FROM v$pdbs ORDER BY con_id`))
			topology["pdb_list_state"] = "collected"
			allContainers := c.queryString(ctx, "oracle.topology.pdb_visibility", `SELECT SYS_CONTEXT('USERENV','ISDBA') FROM dual`) == "TRUE"
			if len(c.errors) != before || topology["connected_container"] != "CDB$ROOT" || !allContainers {
				topology["pdb_list_state"] = "not_collected"
				topology["pdb_list_remediation"] = "请 DBA 在 CDB$ROOT 核对 V_$PDBS 授权及 inspection account 的 CONTAINER_DATA。普通账号成功查询可能仅返回部分 PDB；请 DBA 提供具有全部容器可见性的 PDB 清单，或使用 SYSDBA 重新采集以确认清单完整性。仅巡检当前连接容器。"
			}
		}
	} else {
		topology["pdb_list_state"] = "not_collected"
		topology["pdb_list_remediation"] = "Ask the DBA to grant SELECT on SYS.V_$INSTANCE and rerun collection to detect multitenant capabilities."
	}
}

func probeBoolean(raw string) any {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "YES", "Y", "TRUE":
		return true
	case "NO", "N", "FALSE":
		return false
	default:
		return nil
	}
}
