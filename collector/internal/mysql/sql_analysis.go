package mysql

import "context"

func (c *metricsCollector) collectSQLAnalysis(ctx context.Context) map[string]any {
	limit := effectiveTopN(c.cfg.TopN)
	return map[string]any{
		"top_sql_by_time":         rowsPayload(c.topSQLByTime(ctx, limit)),
		"top_sql_explain":         rowsPayload(c.topSQLExplainCandidates(ctx, limit)),
		"top_sql_by_count":        rowsPayload(c.topSQLByCount(ctx, limit)),
		"top_sql_by_rows_scanned": rowsPayload(c.topSQLByRowsScanned(ctx, limit)),
		"full_scan_sqls":          rowsPayload(c.fullScanSQLs(ctx, limit)),
		"no_index_sqls":           rowsPayload(c.noIndexSQLs(ctx, limit)),
		"tmp_table_sqls":          rowsPayload(c.tmpTableSQLs(ctx, limit)),
	}
}

func (c *metricsCollector) topSQLByTime(ctx context.Context, limit int) []map[string]any {
	query := `
SELECT DIGEST,
       DIGEST_TEXT,
       COUNT_STAR,
       ROUND(SUM_TIMER_WAIT / 1000000000, 3) AS total_time_ms,
       ROUND(AVG_TIMER_WAIT / 1000000000, 3) AS avg_time_ms
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST_TEXT IS NOT NULL
ORDER BY SUM_TIMER_WAIT DESC
LIMIT ?`
	return c.queryRows(ctx, "sql_analysis.top_sql_by_time", query, limit)
}

func (c *metricsCollector) topSQLExplainCandidates(ctx context.Context, limit int) []map[string]any {
	query := `
SELECT DIGEST,
       DIGEST_TEXT,
       SUM_NO_INDEX_USED,
       SUM_NO_GOOD_INDEX_USED,
       'digest summary only; original SQL text required for EXPLAIN' AS explain_note
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST_TEXT IS NOT NULL
ORDER BY SUM_TIMER_WAIT DESC
LIMIT ?`
	return c.queryRows(ctx, "sql_analysis.top_sql_explain", query, limit)
}

func (c *metricsCollector) topSQLByCount(ctx context.Context, limit int) []map[string]any {
	query := `
SELECT DIGEST,
       DIGEST_TEXT,
       COUNT_STAR,
       ROUND(SUM_TIMER_WAIT / 1000000000, 3) AS total_time_ms
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST_TEXT IS NOT NULL
ORDER BY COUNT_STAR DESC
LIMIT ?`
	return c.queryRows(ctx, "sql_analysis.top_sql_by_count", query, limit)
}

func (c *metricsCollector) topSQLByRowsScanned(ctx context.Context, limit int) []map[string]any {
	query := `
SELECT DIGEST,
       DIGEST_TEXT,
       SUM_ROWS_EXAMINED,
       COUNT_STAR,
       ROUND(SUM_ROWS_EXAMINED / NULLIF(COUNT_STAR, 0), 2) AS avg_rows_examined
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST_TEXT IS NOT NULL
ORDER BY SUM_ROWS_EXAMINED DESC
LIMIT ?`
	return c.queryRows(ctx, "sql_analysis.top_sql_by_rows_scanned", query, limit)
}

func (c *metricsCollector) fullScanSQLs(ctx context.Context, limit int) []map[string]any {
	query := `
SELECT DIGEST,
       DIGEST_TEXT,
       SUM_ROWS_EXAMINED,
       SUM_NO_INDEX_USED,
       SUM_NO_GOOD_INDEX_USED
FROM performance_schema.events_statements_summary_by_digest
WHERE SUM_NO_INDEX_USED > 0 OR SUM_NO_GOOD_INDEX_USED > 0
ORDER BY (SUM_NO_INDEX_USED + SUM_NO_GOOD_INDEX_USED) DESC
LIMIT ?`
	return c.queryRows(ctx, "sql_analysis.full_scan_sqls", query, limit)
}

func (c *metricsCollector) noIndexSQLs(ctx context.Context, limit int) []map[string]any {
	query := `
SELECT DIGEST,
       DIGEST_TEXT,
       SUM_NO_INDEX_USED,
       SUM_ROWS_EXAMINED,
       COUNT_STAR
FROM performance_schema.events_statements_summary_by_digest
WHERE SUM_NO_INDEX_USED > 0
ORDER BY SUM_NO_INDEX_USED DESC
LIMIT ?`
	return c.queryRows(ctx, "sql_analysis.no_index_sqls", query, limit)
}

func (c *metricsCollector) tmpTableSQLs(ctx context.Context, limit int) []map[string]any {
	if !c.performanceSchemaTableExists(ctx, "events_statements_summary_by_digest") {
		return []map[string]any{}
	}
	if !c.performanceSchemaColumnExists(ctx, "events_statements_summary_by_digest", "SUM_CREATED_TMP_TABLES") {
		return []map[string]any{}
	}
	if !c.performanceSchemaColumnExists(ctx, "events_statements_summary_by_digest", "SUM_CREATED_TMP_DISK_TABLES") {
		return []map[string]any{}
	}
	query := `
SELECT DIGEST,
       DIGEST_TEXT,
       COUNT_STAR,
       SUM_CREATED_TMP_TABLES,
       SUM_CREATED_TMP_DISK_TABLES
FROM performance_schema.events_statements_summary_by_digest
WHERE SUM_CREATED_TMP_TABLES > 0 OR SUM_CREATED_TMP_DISK_TABLES > 0
ORDER BY SUM_CREATED_TMP_DISK_TABLES DESC, SUM_CREATED_TMP_TABLES DESC, COUNT_STAR DESC
LIMIT ?`
	return c.queryRows(ctx, "sql_analysis.tmp_table_sqls", query, limit)
}
