package mysql

import "context"

func (c *metricsCollector) collectStorage(ctx context.Context) map[string]any {
	databaseSizes := c.queryRows(ctx, "storage.database_sizes", databaseSizesSQL())
	topTables := c.queryRows(ctx, "storage.top_tables", topTablesSQL(), effectiveTopN(c.cfg.TopN))
	topTablesByRows := c.queryRows(ctx, "storage.top_tables_by_rows", topTablesByRowsSQL(), effectiveTopN(c.cfg.TopN))
	wideTables := c.wideTables(ctx)
	mixedEngines := c.mixedEngineTables(ctx)
	autoIncrementUsage := c.autoIncrementUsageRows(ctx)
	topIndexesBySize := c.topIndexesBySize(ctx)
	tablesWithManyIndexes := c.tablesWithManyIndexes(ctx)
	wideCompositeIndexes := c.wideCompositeIndexes(ctx)
	fragmentedMax := c.fragmentedTableMaxRatio(ctx)
	ibdataSize := c.bytesToGiB(c.filesSizeByPattern(ctx, "storage.ibdata1_size_bytes", ibdataPattern))
	undoSize := c.bytesToMiB(c.filesSizeByPattern(ctx, "storage.undo_tablespace_size_bytes", undoTablespacePattern))
	tempSize := c.tempTablespaceGiB(ctx)
	binlogUsage := c.binlogDiskUsageGiB(ctx)
	logFileSizes := c.logFileSizesGiB(ctx)
	return map[string]any{
		"database_sizes":                rowsPayload(databaseSizes),
		"top_tables":                    rowsPayload(topTables),
		"fragmented_tables":             fragmentedMax,
		"ibdata1_size_bytes":            ibdataSize,
		"undo_tablespace_size_bytes":    undoSize,
		"temp_tablespace_size_bytes":    tempSize,
		"binlog_disk_usage_bytes":       binlogUsage,
		"log_file_sizes":                logFileSizes,
		"tables_without_pk":             rowsPayload(c.tablesWithoutPK(ctx)),
		"unused_indexes":                rowsPayload(c.unusedIndexes(ctx)),
		"redundant_indexes":             rowsPayload(c.redundantIndexes(ctx)),
		"top_indexes_by_size":           rowsPayload(topIndexesBySize),
		"tables_with_many_indexes":      rowsPayloadWithStats(tablesWithManyIndexes, maxFieldValue(tablesWithManyIndexes, "index_count", "INDEX_COUNT")),
		"wide_composite_indexes":        rowsPayloadWithStats(wideCompositeIndexes, maxFieldValue(wideCompositeIndexes, "column_count", "COLUMN_COUNT")),
		"wide_tables":                   rowsPayloadWithStats(wideTables, maxFieldValue(wideTables, "column_count", "COLUMN_COUNT")),
		"large_tables_without_index":    c.largeTablesWithoutIndex(ctx),
		"foreign_keys":                  c.foreignKeyCount(ctx),
		"mixed_engines":                 rowsPayloadWithStats(mixedEngines, float64(len(mixedEngines))),
		"triggers_procedures_events":    c.triggerProcedureEventCounts(ctx),
		"table_index_counts":            c.tableIndexCounts(ctx),
		"auto_increment_usage":          rowsPayloadWithStats(autoIncrementUsage, maxFieldValue(autoIncrementUsage, "usage_percent", "USAGE_PERCENT")),
		"binlog_growth_rate_mb_per_day": c.binlogGrowthMBPerDay(ctx),
		"top_tables_by_rows":            rowsPayload(topTablesByRows),
	}
}

func databaseSizesSQL() string {
	return `
SELECT table_schema,
       ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
FROM information_schema.tables
GROUP BY table_schema
ORDER BY size_mb DESC`
}

func topTablesSQL() string {
	return `
SELECT table_schema,
       table_name,
       ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb,
       table_rows
FROM information_schema.tables
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY (data_length + index_length) DESC
LIMIT ?`
}

func topTablesByRowsSQL() string {
	return `
SELECT table_schema,
       table_name,
       table_rows
FROM information_schema.tables
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY table_rows DESC
LIMIT ?`
}

func (c *metricsCollector) fragmentedTableMaxRatio(ctx context.Context) float64 {
	query := `
SELECT COALESCE(MAX((data_free / NULLIF(data_length + index_length, 0)) * 100), 0)
FROM information_schema.tables
WHERE engine = 'InnoDB'
  AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`
	return c.queryFloat64(ctx, "storage.fragmented_tables", query)
}

func (c *metricsCollector) filesSizeByPattern(ctx context.Context, scope string, pattern string) float64 {
	query := `
SELECT COALESCE(SUM(data_length), 0)
FROM information_schema.files
WHERE file_name LIKE ?`
	return c.queryFloat64(ctx, scope, query, pattern)
}

func (c *metricsCollector) tempTablespaceGiB(ctx context.Context) float64 {
	tempSize := c.filesSizeByPattern(ctx, "storage.temp_tablespace_size_bytes", tempTablespacePattern)
	if tempSize > 0 {
		return c.bytesToGiB(tempSize)
	}
	ibtmpSize := c.filesSizeByPattern(ctx, "storage.temp_tablespace_size_bytes.ibtmp", ibtmpPattern)
	return c.bytesToGiB(ibtmpSize)
}

func (c *metricsCollector) tablesWithoutPK(ctx context.Context) []map[string]any {
	query := `
SELECT t.table_schema, t.table_name
FROM information_schema.tables t
LEFT JOIN information_schema.table_constraints tc
  ON tc.table_schema = t.table_schema
 AND tc.table_name = t.table_name
 AND tc.constraint_type = 'PRIMARY KEY'
WHERE t.table_type = 'BASE TABLE'
  AND t.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND tc.constraint_name IS NULL
LIMIT ?`
	return c.queryRows(ctx, "storage.tables_without_pk", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) unusedIndexes(ctx context.Context) []map[string]any {
	query := `
SELECT object_schema, object_name, index_name, count_fetch
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE index_name IS NOT NULL AND index_name <> 'PRIMARY' AND count_fetch = 0
ORDER BY object_schema, object_name
LIMIT ?`
	return c.queryRows(ctx, "storage.unused_indexes", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) redundantIndexes(ctx context.Context) []map[string]any {
	query := `
SELECT s1.table_schema,
       s1.table_name,
       s1.index_name AS index_name,
       s2.index_name AS covered_by
FROM information_schema.statistics s1
JOIN information_schema.statistics s2
  ON s1.table_schema = s2.table_schema
 AND s1.table_name = s2.table_name
 AND s1.seq_in_index = 1
 AND s2.seq_in_index = 1
 AND s1.index_name <> s2.index_name
 AND s1.column_name = s2.column_name
WHERE s1.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
LIMIT ?`
	return c.queryRows(ctx, "storage.redundant_indexes", query, effectiveTopN(c.cfg.TopN))
}

func (c *metricsCollector) largeTablesWithoutIndex(ctx context.Context) int64 {
	query := `
SELECT COUNT(*)
FROM information_schema.tables t
WHERE t.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND t.table_rows > ?
  AND NOT EXISTS (
    SELECT 1
    FROM information_schema.statistics s
    WHERE s.table_schema = t.table_schema
      AND s.table_name = t.table_name
      AND s.non_unique = 0
  )`
	return c.queryInt64(ctx, "storage.large_tables_without_index", query, largeTableRowsThreshold)
}

func (c *metricsCollector) foreignKeyCount(ctx context.Context) int64 {
	query := `
SELECT COUNT(*)
FROM information_schema.referential_constraints
WHERE constraint_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`
	return c.queryInt64(ctx, "storage.foreign_keys", query)
}

func (c *metricsCollector) triggerProcedureEventCounts(ctx context.Context) map[string]any {
	triggers := c.queryInt64(ctx, "storage.triggers", "SELECT COUNT(*) FROM information_schema.triggers")
	procedures := c.queryInt64(ctx, "storage.procedures", `
SELECT COUNT(*)
FROM information_schema.routines
WHERE routine_type = 'PROCEDURE'`)
	events := c.queryInt64(ctx, "storage.events", "SELECT COUNT(*) FROM information_schema.events")
	return map[string]any{"triggers": triggers, "procedures": procedures, "events": events}
}

func (c *metricsCollector) tableIndexCounts(ctx context.Context) map[string]any {
	tables := c.queryInt64(ctx, "storage.table_index_counts.tables", `
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`)
	indexes := c.queryInt64(ctx, "storage.table_index_counts.indexes", `
SELECT COUNT(*)
FROM information_schema.statistics
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')`)
	return map[string]any{"tables": tables, "indexes": indexes}
}

func (c *metricsCollector) binlogGrowthMBPerDay(ctx context.Context) float64 {
	bytesWritten := c.statusFloat64(ctx, "Binlog_bytes_written")
	uptime := c.statusInt64(ctx, "Uptime")
	if uptime <= 0 {
		return 0
	}
	growthPerDay := (bytesWritten / float64(uptime)) * secondsPerDay
	return growthPerDay / bytesPerMiB
}
