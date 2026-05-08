package gaussdb

import (
	"fmt"
	"strings"
)

const lockBudgetMinimum = 1000000

type sqlCheckSpec struct {
	Name    string
	Query   string
	Project func(item itemSpec, rows []map[string]any, duration int64) itemRecord
}

var sqlCheckByName = map[string]sqlCheckSpec{
	"CheckGaussVer":        {Name: "CheckGaussVer", Query: `SELECT version() AS "version"`, Project: projectVersion},
	"CheckDBConnection":    {Name: "CheckDBConnection", Query: `SELECT current_database() AS "database", current_user AS "user", version() AS "version"`, Project: projectConnection},
	"CheckClusterState":    {Name: "CheckClusterState", Query: `SELECT node_name AS "node_name", node_type AS "node_type", node_host AS "node_host", node_port AS "node_port" FROM pg_catalog.pgxc_node ORDER BY node_name`, Project: projectRowsNormal("cluster node metadata available")},
	"CheckReadonlyMode":    {Name: "CheckReadonlyMode", Query: `SELECT current_setting('default_transaction_read_only') AS "readonly"`, Project: projectReadonly},
	"CheckDBParams":        {Name: "CheckDBParams", Query: `SELECT name AS "name", setting AS "setting" FROM pg_catalog.pg_settings ORDER BY name`, Project: projectRowsNormal("database parameters collected")},
	"CheckGUCValue":        {Name: "CheckGUCValue", Query: gucValueQuery, Project: projectGUCValue},
	"CheckTableSpace":      {Name: "CheckTableSpace", Query: `SELECT spcname AS "tablespace", pg_catalog.pg_tablespace_location(oid) AS "location" FROM pg_catalog.pg_tablespace ORDER BY spcname`, Project: projectRowsNormal("tablespace metadata collected")},
	"CheckHashIndex":       {Name: "CheckHashIndex", Query: hashIndexQuery, Project: projectZeroCountNormal("hash index count")},
	"CheckSysTable":        {Name: "CheckSysTable", Query: sysTableQuery, Project: projectSysTable},
	"CheckKeyDBTableSize":  {Name: "CheckKeyDBTableSize", Query: `SELECT current_database() AS "database", pg_catalog.pg_database_size(current_database()) AS "size_bytes"`, Project: projectRowsNormal("database size collected")},
	"CheckCurConnCount":    {Name: "CheckCurConnCount", Query: currentConnectionQuery, Project: projectConnectionCount},
	"CheckCursorNum":       {Name: "CheckCursorNum", Query: `SELECT count(*) AS "count" FROM pg_catalog.pg_cursors`, Project: projectSingleCount("open cursor count")},
	"CheckLockNum":         {Name: "CheckLockNum", Query: `SELECT count(*) AS "count" FROM pg_catalog.pg_locks`, Project: projectSingleCount("lock count")},
	"CheckIdleSession":     {Name: "CheckIdleSession", Query: idleSessionQuery, Project: projectZeroCountNormal("idle session count")},
	"CheckPgPreparedXacts": {Name: "CheckPgPreparedXacts", Query: `SELECT count(*) AS "count" FROM pg_catalog.pg_prepared_xacts`, Project: projectZeroCountNormal("prepared transaction count")},
	"CheckWorkloadTrx":     {Name: "CheckWorkloadTrx", Query: workloadTransactionQuery, Project: projectZeroCountNormal("long transaction count")},
	"CheckDBStat":          {Name: "CheckDBStat", Query: dbStatQuery, Project: projectRowsNormal("database statistics collected")},
	"CheckBPHitRatio":      {Name: "CheckBPHitRatio", Query: bpHitRatioQuery, Project: projectRowsNormal("buffer hit ratio collected")},
	"CheckReturnType":      {Name: "CheckReturnType", Query: userFunctionQuery, Project: projectSingleCount("user-defined function count")},
	"CheckPgxcRedistb":     {Name: "CheckPgxcRedistb", Query: redistbQuery, Project: projectRowsNormal("pgxc_redistb lookup completed")},
	"CheckNodeGroupName":   {Name: "CheckNodeGroupName", Query: nodeGroupQuery, Project: projectRowsNormal("node group metadata collected")},
}

const gucValueQuery = `SELECT
  max(CASE WHEN name = 'max_locks_per_transaction' THEN setting::bigint END) AS "max_locks_per_transaction",
  max(CASE WHEN name = 'max_connections' THEN setting::bigint END) AS "max_connections",
  max(CASE WHEN name = 'max_prepared_transactions' THEN setting::bigint END) AS "max_prepared_transactions"
FROM pg_catalog.pg_settings
WHERE name IN ('max_locks_per_transaction', 'max_connections', 'max_prepared_transactions')`

const hashIndexQuery = `SELECT count(*) AS "count"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_am am ON c.relam = am.oid
WHERE c.relkind = 'i' AND lower(am.amname) LIKE '%hash%'`

const sysTableQuery = `SELECT n.nspname AS "schema", c.relname AS "table_name", c.relpages AS "pages", c.reltuples AS "rows"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'pg_catalog' AND c.relkind = 'r'
ORDER BY c.relpages DESC, c.relname
LIMIT 20`

const currentConnectionQuery = `SELECT
  (SELECT count(*) FROM pg_catalog.pg_stat_activity) AS "current_connections",
  current_setting('max_connections')::bigint AS "max_connections"`

const idleSessionQuery = `SELECT count(*) AS "count"
FROM pg_catalog.pg_stat_activity
WHERE state = 'idle' AND pid <> pg_catalog.pg_backend_pid()`

const workloadTransactionQuery = `SELECT count(*) AS "count"
FROM pg_catalog.pg_stat_activity
WHERE xact_start IS NOT NULL AND now() - xact_start > interval '30 minutes'`

const dbStatQuery = `SELECT datname AS "database", numbackends AS "numbackends", xact_commit AS "xact_commit",
       xact_rollback AS "xact_rollback", blks_read AS "blks_read", blks_hit AS "blks_hit",
       tup_returned AS "tup_returned", tup_fetched AS "tup_fetched", deadlocks AS "deadlocks"
FROM pg_catalog.pg_stat_database
ORDER BY datname`

const bpHitRatioQuery = `SELECT datname AS "database",
       CASE WHEN blks_hit + blks_read = 0 THEN 1
            ELSE round(blks_hit::numeric / (blks_hit + blks_read), 6)
       END AS "hit_ratio"
FROM pg_catalog.pg_stat_database
ORDER BY datname`

const userFunctionQuery = `SELECT count(*) AS "count"
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')`

const redistbQuery = `SELECT count(*) AS "count"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'pg_catalog' AND c.relname = 'pgxc_redistb'`

const nodeGroupQuery = `SELECT count(*) AS "count"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'pg_catalog' AND c.relname = 'pgxc_group'`

func projectVersion(item itemSpec, rows []map[string]any, duration int64) itemRecord {
	version := firstValue(rows, "version")
	return normalRecord(item, "GaussDB version: "+version, map[string]any{"version": version}, duration)
}

func projectConnection(item itemSpec, rows []map[string]any, duration int64) itemRecord {
	details := firstRow(rows)
	summary := "Database connection is normal."
	return normalRecord(item, summary, details, duration)
}

func projectReadonly(item itemSpec, rows []map[string]any, duration int64) itemRecord {
	readonly := strings.TrimSpace(firstValue(rows, "readonly"))
	if strings.EqualFold(readonly, "on") {
		return abnormalRecord(item, "default_transaction_read_only is on", firstRow(rows), duration)
	}
	return normalRecord(item, readonly, firstRow(rows), duration)
}

func projectGUCValue(item itemSpec, rows []map[string]any, duration int64) itemRecord {
	row := firstRow(rows)
	budget := numeric(row["max_locks_per_transaction"]) * (numeric(row["max_connections"]) + numeric(row["max_prepared_transactions"]))
	row["computed_value"] = budget
	summary := fmt.Sprintf("max_locks_per_transaction * (max_connections + max_prepared_transactions) = %d", budget)
	if budget < lockBudgetMinimum {
		return abnormalRecord(item, summary+" below 1000000", row, duration)
	}
	return normalRecord(item, summary, row, duration)
}

func projectSysTable(item itemSpec, rows []map[string]any, duration int64) itemRecord {
	details := map[string]any{"table_count": len(rows), "tables": rows}
	return normalRecord(item, fmt.Sprintf("collected %d pg_catalog tables", len(rows)), details, duration)
}

func projectConnectionCount(item itemSpec, rows []map[string]any, duration int64) itemRecord {
	row := firstRow(rows)
	current := numeric(row["current_connections"])
	maximum := numeric(row["max_connections"])
	row["usage_percent"] = percent(current, maximum)
	return normalRecord(item, fmt.Sprintf("%d/%d connections", current, maximum), row, duration)
}

func projectRowsNormal(summary string) func(itemSpec, []map[string]any, int64) itemRecord {
	return func(item itemSpec, rows []map[string]any, duration int64) itemRecord {
		return normalRecord(item, summary, map[string]any{"row_count": len(rows), "rows": rows}, duration)
	}
}

func projectSingleCount(label string) func(itemSpec, []map[string]any, int64) itemRecord {
	return func(item itemSpec, rows []map[string]any, duration int64) itemRecord {
		count := numeric(firstRow(rows)["count"])
		return normalRecord(item, fmt.Sprintf("%s: %d", label, count), map[string]any{"count": count}, duration)
	}
}

func projectZeroCountNormal(label string) func(itemSpec, []map[string]any, int64) itemRecord {
	return func(item itemSpec, rows []map[string]any, duration int64) itemRecord {
		count := numeric(firstRow(rows)["count"])
		if count > 0 {
			return abnormalRecord(item, fmt.Sprintf("%s: %d", label, count), map[string]any{"count": count}, duration)
		}
		return normalRecord(item, fmt.Sprintf("%s: 0", label), map[string]any{"count": count}, duration)
	}
}

func normalRecord(item itemSpec, summary string, details map[string]any, duration int64) itemRecord {
	return statusRecord(item, statusOK, "normal", summary, details, duration)
}

func abnormalRecord(item itemSpec, summary string, details map[string]any, duration int64) itemRecord {
	return statusRecord(item, statusNG, "abnormal", summary, details, duration)
}

func notApplicableRecord(item itemSpec) itemRecord {
	details := map[string]any{"reason": "SQL-first mode does not collect this gs_check-only item"}
	return statusRecord(item, statusNone, "not_applicable", "not applicable in SQL-first mode", details, 0)
}

func failedSQLRecord(item itemSpec, query string, duration int64, err error) itemRecord {
	details := map[string]any{"error": err.Error()}
	record := abnormalRecord(item, err.Error(), details, duration)
	record.Command = query
	return record
}

func statusRecord(item itemSpec, status string, normalized string, summary string, details map[string]any, duration int64) itemRecord {
	return itemRecord{
		Item: item.Name, Domain: item.Domain, Label: item.Label,
		Status: status, NormalizedStatus: normalized, Summary: summary,
		Details: details, DurationMS: duration,
	}
}

func firstRow(rows []map[string]any) map[string]any {
	if len(rows) == 0 {
		return map[string]any{}
	}
	return rows[0]
}

func firstValue(rows []map[string]any, key string) string {
	return strings.TrimSpace(formatAny(firstRow(rows)[key]))
}

func numeric(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		var parsed int
		_, _ = fmt.Sscanf(strings.TrimSpace(typed), "%d", &parsed)
		return parsed
	default:
		return 0
	}
}

func percent(current int, maximum int) float64 {
	if maximum == 0 {
		return 0
	}
	return float64(current) / float64(maximum) * 100
}

func extractKernelVersion(content string) string {
	fields := strings.Fields(content)
	for index, field := range fields {
		if field == "Kernel" && index+1 < len(fields) {
			return strings.TrimSpace(fields[index+1])
		}
	}
	return ""
}
