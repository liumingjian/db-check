package oracle

import (
	"context"
	"strconv"
	"strings"
)

// Probes use unlicensed current-state views and stay in the connected container.
func (c *metricsCollector) collectDeployment(ctx context.Context) (map[string]any, map[string]any) {
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
	version := map[string]any{
		"version": raw, "major": major, "family": family,
		"capabilities": map[string]any{
			"multitenant":        multitenant,
			"diagnostic_alert":   multitenant,
			"sql_patch_registry": multitenant,
		},
	}
	role := c.queryString(ctx, "oracle.topology.database_role", `SELECT database_role FROM v$database`)
	normalizedRole := "unknown"
	if role == "PRIMARY" {
		normalizedRole = "primary"
	} else if strings.Contains(role, "STANDBY") {
		normalizedRole = "standby"
	}
	topology := map[string]any{
		"is_rac":        probeBoolean(c.queryString(ctx, "oracle.topology.is_rac", `SELECT value FROM v$parameter WHERE name='cluster_database'`)),
		"database_role": role, "role": normalizedRole,
		"is_cdb":              nil,
		"is_asm":              probeBoolean(c.queryString(ctx, "oracle.topology.is_asm", `SELECT CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM (SELECT name FROM v$datafile UNION ALL SELECT name FROM v$tempfile UNION ALL SELECT member AS name FROM v$logfile) WHERE name LIKE '+%'`)),
		"connected_container": "", "inspection_scope": "connected_container",
		"pdbs": rowsPayload([]map[string]any{}), "pdb_list_state": "not_applicable",
	}
	if major == 11 {
		topology["is_cdb"] = false
	} else if multitenant {
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
			if len(c.errors) != before || topology["connected_container"] != "CDB$ROOT" {
				topology["pdb_list_state"] = "not_collected"
				topology["pdb_list_remediation"] = "Ask the DBA to connect the inspection account to CDB$ROOT with access to V_$PDBS, then collect again to list every PDB. Only the connected container is inspected."
			}
		}
	} else {
		topology["pdb_list_state"] = "not_collected"
		topology["pdb_list_remediation"] = "Ask the DBA to grant SELECT on SYS.V_$INSTANCE and rerun collection to detect multitenant capabilities."
	}
	return version, topology
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
