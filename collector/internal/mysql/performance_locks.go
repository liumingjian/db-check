package mysql

import "context"

func (c *metricsCollector) metadataLockWaitRows(ctx context.Context) []map[string]any {
	if !c.performanceSchemaTableExists(ctx, "metadata_locks") {
		return []map[string]any{}
	}
	if !c.performanceSchemaTableExists(ctx, "threads") {
		query := `
SELECT m.OWNER_THREAD_ID AS processlist_id,
       m.OBJECT_TYPE,
       m.OBJECT_SCHEMA,
       m.OBJECT_NAME,
       m.LOCK_TYPE,
       m.LOCK_DURATION,
       m.LOCK_STATUS
FROM performance_schema.metadata_locks m
WHERE m.LOCK_STATUS = 'PENDING'
ORDER BY processlist_id
LIMIT ?`
		return c.queryRows(ctx, "performance.metadata_lock_waits", query, effectiveTopN(c.cfg.TopN))
	}
	query := `
SELECT COALESCE(t.PROCESSLIST_ID, m.OWNER_THREAD_ID) AS processlist_id,
       m.OBJECT_TYPE,
       m.OBJECT_SCHEMA,
       m.OBJECT_NAME,
       m.LOCK_TYPE,
       m.LOCK_DURATION,
       m.LOCK_STATUS
FROM performance_schema.metadata_locks m
LEFT JOIN performance_schema.threads t
  ON t.THREAD_ID = m.OWNER_THREAD_ID
WHERE m.LOCK_STATUS = 'PENDING'
ORDER BY processlist_id
LIMIT ?`
	return c.queryRows(ctx, "performance.metadata_lock_waits", query, effectiveTopN(c.cfg.TopN))
}
