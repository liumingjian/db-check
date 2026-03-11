package mysql

import (
	"context"
	"database/sql"
	"strings"
)

const (
	deadlockSnippetMaxChars = 2048
)

func (c *metricsCollector) collectPerformance(ctx context.Context) map[string]any {
	uptime := c.statusInt64(ctx, "Uptime")
	maxConnections := c.variableInt64(ctx, "max_connections")
	threadsConnected := c.statusInt64(ctx, "Threads_connected")
	threadsRunning := c.statusInt64(ctx, "Threads_running")
	abortedConnects := c.statusInt64(ctx, "Aborted_connects")
	slowQueries := c.statusInt64(ctx, "Slow_queries")
	fullScanRatio := c.calcFullScanRatio(ctx)
	innodbData := c.collectInnoDBPerformance(ctx)
	metadataLockWaits := c.metadataLockWaitRows(ctx)
	topTablesByIO := c.topTablesByIO(ctx)
	fullScanTables := c.fullScanTables(ctx)
	rowOpsTopTables := c.rowOpsTopTables(ctx)
	return map[string]any{
		"threads_running":            threadsRunning,
		"connection_usage_percent":   safePercent(float64(threadsConnected), float64(maxConnections)),
		"aborted_connects":           abortedConnects,
		"slow_queries_count":         slowQueries,
		"full_scan_ratio":            fullScanRatio,
		"innodb":                     innodbData,
		"thread_cache_hit_ratio":     c.calcThreadCacheHitRatio(ctx),
		"opened_tables_per_second":   safePerSecond(float64(c.statusInt64(ctx, "Opened_tables")), uptime),
		"tmp_disk_table_ratio":       c.calcTmpDiskTableRatio(ctx),
		"sort_merge_passes":          c.statusInt64(ctx, "Sort_merge_passes"),
		"top_wait_events":            rowsPayload(c.collectTopWaitEvents(ctx)),
		"top_tables_by_io":          rowsPayload(topTablesByIO),
		"full_scan_tables":          rowsPayload(fullScanTables),
		"row_ops_top_tables":        rowsPayload(rowOpsTopTables),
		"qps":                        safePerSecond(float64(c.statusInt64(ctx, "Questions")), uptime),
		"max_used_connections_ratio": safePercent(float64(c.statusInt64(ctx, "Max_used_connections")), float64(maxConnections)),
		"deadlock_frequency":         c.calcDeadlockFrequency(ctx, uptime),
		"latest_deadlock_info":       c.latestDeadlockInfo(ctx),
		"current_lock_waits":         c.currentLockWaits(ctx),
		"long_transactions":          c.longTransactions(ctx),
		"metadata_lock_waits":        rowsPayloadWithStats(metadataLockWaits, float64(len(metadataLockWaits))),
		"innodb_lock_wait_timeout":   c.variableInt64(ctx, "innodb_lock_wait_timeout"),
		"row_lock_waits_delta":       c.statusInt64(ctx, "Innodb_row_lock_waits"),
		"row_lock_time_stats":        c.rowLockTimeStats(ctx),
	}
}

func (c *metricsCollector) calcFullScanRatio(ctx context.Context) float64 {
	fullScanReads := c.statusFloat64(ctx, "Handler_read_rnd_next")
	selectOps := c.statusFloat64(ctx, "Com_select")
	return safePercent(fullScanReads, selectOps)
}

func (c *metricsCollector) collectInnoDBPerformance(ctx context.Context) map[string]any {
	readRequests := c.statusFloat64(ctx, "Innodb_buffer_pool_read_requests")
	reads := c.statusFloat64(ctx, "Innodb_buffer_pool_reads")
	dirtyPages := c.statusFloat64(ctx, "Innodb_buffer_pool_pages_dirty")
	totalPages := c.statusFloat64(ctx, "Innodb_buffer_pool_pages_total")
	rowLockTime := c.statusFloat64(ctx, "Innodb_row_lock_time")
	rowLockWaits := c.statusFloat64(ctx, "Innodb_row_lock_waits")
	return map[string]any{
		"buffer_pool_hit_ratio": safePercent(readRequests-reads, readRequests),
		"dirty_page_ratio":      safePercent(dirtyPages, totalPages),
		"row_lock_time_avg_ms":  safeRatio(rowLockTime, rowLockWaits),
	}
}

func (c *metricsCollector) calcThreadCacheHitRatio(ctx context.Context) float64 {
	connections := c.statusFloat64(ctx, "Connections")
	threadsCreated := c.statusFloat64(ctx, "Threads_created")
	return safePercent(connections-threadsCreated, connections)
}

func (c *metricsCollector) calcTmpDiskTableRatio(ctx context.Context) float64 {
	diskTables := c.statusFloat64(ctx, "Created_tmp_disk_tables")
	allTables := c.statusFloat64(ctx, "Created_tmp_tables")
	return safePercent(diskTables, allTables)
}

func (c *metricsCollector) calcDeadlockFrequency(ctx context.Context, uptime int64) float64 {
	deadlocks := float64(c.statusInt64(ctx, "Innodb_deadlocks"))
	uptimeHours := float64(uptime) / secondsPerHour
	return safeRatio(deadlocks, uptimeHours)
}

func (c *metricsCollector) collectTopWaitEvents(ctx context.Context) []map[string]any {
	query := `
SELECT EVENT_NAME,
       COUNT_STAR,
       ROUND(SUM_TIMER_WAIT / 1000000000, 3) AS total_wait_ms
FROM performance_schema.events_waits_summary_global_by_event_name
WHERE COUNT_STAR > 0
ORDER BY SUM_TIMER_WAIT DESC
LIMIT ?`
	return c.queryRows(ctx, "performance.top_wait_events", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) latestDeadlockInfo(ctx context.Context) string {
	query := "SHOW ENGINE INNODB STATUS"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		c.addErr("performance.latest_deadlock_info", err)
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		var engineType sql.NullString
		var name sql.NullString
		var status sql.NullString
		if err := rows.Scan(&engineType, &name, &status); err != nil {
			c.addErr("performance.latest_deadlock_info", err)
			return ""
		}
		return extractDeadlockSnippet(status.String)
	}
	return ""
}

func extractDeadlockSnippet(text string) string {
	const marker = "LATEST DETECTED DEADLOCK"
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	idx := strings.Index(trimmed, marker)
	if idx < 0 {
		if len(trimmed) <= deadlockSnippetMaxChars {
			return trimmed
		}
		return trimmed[:deadlockSnippetMaxChars]
	}
	snippet := trimmed[idx:]
	if len(snippet) <= deadlockSnippetMaxChars {
		return snippet
	}
	return snippet[:deadlockSnippetMaxChars]
}

func (c *metricsCollector) currentLockWaits(ctx context.Context) int64 {
	if c.performanceSchemaTableExists(ctx, "data_lock_waits") {
		query := "SELECT COUNT(*) FROM performance_schema.data_lock_waits"
		count := c.queryInt64(ctx, "performance.current_lock_waits", query)
		if count > 0 {
			return count
		}
	}
	fallback := "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'"
	return c.queryInt64(ctx, "performance.current_lock_waits.fallback", fallback)
}

func (c *metricsCollector) longTransactions(ctx context.Context) int64 {
	query := `
SELECT COUNT(*)
FROM information_schema.innodb_trx
WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > ?`
	return c.queryInt64(ctx, "performance.long_transactions", query, longTransactionSeconds)
}

func (c *metricsCollector) rowLockTimeStats(ctx context.Context) map[string]any {
	totalMs := c.statusFloat64(ctx, "Innodb_row_lock_time")
	waitCount := c.statusFloat64(ctx, "Innodb_row_lock_waits")
	maxMs := c.statusFloat64(ctx, "Innodb_row_lock_time_max")
	avgMs := safeRatio(totalMs, waitCount)
	return map[string]any{
		"total_ms": totalMs,
		"avg_ms":   avgMs,
		"max_ms":   maxMs,
		"waits":    waitCount,
	}
}
