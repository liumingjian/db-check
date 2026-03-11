package mysql

import "context"

const recentErrorLimit = 20

func (c *metricsCollector) collectBasicInfo(ctx context.Context) map[string]any {
	uptime := c.statusInt64(ctx, "Uptime")
	recentErrors := c.collectRecentErrors(ctx)
	versionInfo := c.collectVersionInfo(ctx)
	return map[string]any{
		"is_alive":                true,
		"is_connectable":          true,
		"uptime_seconds":          uptime,
		"version":                 versionInfo["version"],
		"version_comment":         versionInfo["version_comment"],
		"version_vars":            versionInfo["version_vars"],
		"datadir":                 c.variableString(ctx, "datadir"),
		"socket":                  c.variableString(ctx, "socket"),
		"log_error":               c.variableString(ctx, "log_error"),
		"crash_recovery_detected": c.detectCrashRecovery(ctx),
		"recent_errors":           rowsPayload(recentErrors),
		"connectivity_status":     "reachable",
		"db_host":                 c.cfg.DBHost,
		"db_port":                 c.cfg.DBPort,
		"db_name":                 c.cfg.DBName,
		"top_n":                   c.cfg.TopN,
	}
}

func (c *metricsCollector) collectVersionInfo(ctx context.Context) map[string]any {
	version := c.variableString(ctx, "version")
	return map[string]any{
		"version":         version,
		"version_comment": c.variableString(ctx, "version_comment"),
		"version_vars": map[string]any{
			"version":                 version,
			"version_compile_os":      c.variableString(ctx, "version_compile_os"),
			"version_compile_machine": c.variableString(ctx, "version_compile_machine"),
		},
	}
}

func (c *metricsCollector) collectRecentErrors(ctx context.Context) []map[string]any {
	if !c.performanceSchemaTableExists(ctx, "error_log") {
		return []map[string]any{}
	}
	query := `
SELECT LOGGED, PRIO, ERROR_CODE, SUBSYSTEM, DATA
FROM performance_schema.error_log
WHERE PRIO IN ('Error','Warning')
ORDER BY LOGGED DESC
LIMIT ?`
	limit := recentErrorLimit
	if c.cfg.TopN > 0 && c.cfg.TopN < recentErrorLimit {
		limit = c.cfg.TopN
	}
	return c.queryRows(ctx, "basic_info.recent_errors", query, limit)
}

func (c *metricsCollector) detectCrashRecovery(ctx context.Context) bool {
	if !c.performanceSchemaTableExists(ctx, "error_log") {
		return false
	}
	query := `
SELECT COUNT(*)
FROM performance_schema.error_log
WHERE LOWER(DATA) LIKE '%crash recovery%'
  AND LOGGED >= NOW() - INTERVAL 1 DAY`
	count := c.queryInt64(ctx, "basic_info.crash_recovery_detected", query)
	return count > 0
}

func (c *metricsCollector) collectConfigCheck(ctx context.Context) map[string]any {
	transactionIsolation := c.variableString(ctx, "transaction_isolation", "tx_isolation")
	characterSet := map[string]any{
		"server":             c.variableString(ctx, "character_set_server"),
		"database":           c.variableString(ctx, "character_set_database"),
		"client":             c.variableString(ctx, "character_set_client"),
		"connection":         c.variableString(ctx, "character_set_connection"),
		"results":            c.variableString(ctx, "character_set_results"),
		"collation_server":   c.variableString(ctx, "collation_server"),
		"collation_database": c.variableString(ctx, "collation_database"),
	}
	return map[string]any{
		"slow_query_log":                 normalizeOnOff(c.variableString(ctx, "slow_query_log")),
		"long_query_time":                parseFloat64(c.variableString(ctx, "long_query_time")),
		"performance_schema":             normalizeOnOff(c.variableString(ctx, "performance_schema")),
		"innodb_buffer_pool_size":        c.variableInt64(ctx, "innodb_buffer_pool_size"),
		"innodb_log_file_size":           c.variableInt64(ctx, "innodb_log_file_size", "innodb_redo_log_capacity"),
		"innodb_flush_log_at_trx_commit": c.variableInt64(ctx, "innodb_flush_log_at_trx_commit"),
		"sync_binlog":                    c.variableInt64(ctx, "sync_binlog"),
		"innodb_file_per_table":          normalizeOnOff(c.variableString(ctx, "innodb_file_per_table")),
		"innodb_flush_method":            c.variableString(ctx, "innodb_flush_method"),
		"max_connections":                c.variableInt64(ctx, "max_connections"),
		"innodb_buffer_pool_instances":   c.variableInt64(ctx, "innodb_buffer_pool_instances"),
		"innodb_io_capacity":             c.variableInt64(ctx, "innodb_io_capacity"),
		"innodb_page_cleaners":           c.variableInt64(ctx, "innodb_page_cleaners"),
		"innodb_undo_tablespaces":        c.variableInt64(ctx, "innodb_undo_tablespaces"),
		"innodb_undo_log_truncate":       normalizeOnOff(c.variableString(ctx, "innodb_undo_log_truncate")),
		"tmp_table_size":                 c.variableInt64(ctx, "tmp_table_size"),
		"join_buffer_size":               c.variableInt64(ctx, "join_buffer_size"),
		"sort_buffer_size":               c.variableInt64(ctx, "sort_buffer_size"),
		"datadir":                        c.variableString(ctx, "datadir"),
		"socket":                         c.variableString(ctx, "socket"),
		"log_error":                      c.variableString(ctx, "log_error"),
		"character_set":                  characterSet,
		"lower_case_table_names":         c.variableInt64(ctx, "lower_case_table_names"),
		"transaction_isolation":          transactionIsolation,
		"innodb_print_all_deadlocks":     normalizeOnOff(c.variableString(ctx, "innodb_print_all_deadlocks")),
		"innodb_deadlock_detect":         normalizeOnOff(c.variableString(ctx, "innodb_deadlock_detect")),
	}
}
