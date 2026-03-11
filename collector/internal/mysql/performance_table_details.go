package mysql

import "context"

func (c *metricsCollector) topTablesByIO(ctx context.Context) []map[string]any {
	if !c.performanceSchemaTableExists(ctx, "table_io_waits_summary_by_table") {
		return []map[string]any{}
	}
	query := `
SELECT object_schema,
       object_name,
       COUNT_READ AS read_ops,
       COUNT_WRITE AS write_ops,
       ROUND((SUM_TIMER_READ + SUM_TIMER_WRITE) / 1000000000, 3) AS total_wait_ms
FROM performance_schema.table_io_waits_summary_by_table
WHERE object_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY (SUM_TIMER_READ + SUM_TIMER_WRITE) DESC, object_schema, object_name
LIMIT ?`
	return c.queryRows(ctx, "performance.top_tables_by_io", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) fullScanTables(ctx context.Context) []map[string]any {
	if !c.performanceSchemaTableExists(ctx, "table_io_waits_summary_by_index_usage") {
		return []map[string]any{}
	}
	query := `
SELECT object_schema,
       object_name,
       COUNT_READ AS read_ops,
       ROUND(SUM_TIMER_READ / 1000000000, 3) AS read_wait_ms
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE object_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND index_name IS NULL
  AND COUNT_READ > 0
ORDER BY COUNT_READ DESC, object_schema, object_name
LIMIT ?`
	return c.queryRows(ctx, "performance.full_scan_tables", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) rowOpsTopTables(ctx context.Context) []map[string]any {
	if !c.performanceSchemaTableExists(ctx, "table_io_waits_summary_by_table") {
		return []map[string]any{}
	}
	query := `
SELECT object_schema,
       object_name,
       COUNT_FETCH AS fetch_ops,
       COUNT_INSERT AS insert_ops,
       COUNT_UPDATE AS update_ops,
       COUNT_DELETE AS delete_ops,
       (COUNT_FETCH + COUNT_INSERT + COUNT_UPDATE + COUNT_DELETE) AS total_ops
FROM performance_schema.table_io_waits_summary_by_table
WHERE object_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY total_ops DESC, object_schema, object_name
LIMIT ?`
	return c.queryRows(ctx, "performance.row_ops_top_tables", query, effectiveTopN(c.cfg.TopN))
}
