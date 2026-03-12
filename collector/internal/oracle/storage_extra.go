package oracle

import "context"

func (c *metricsCollector) collectStorageExtra(ctx context.Context) map[string]any {
	return map[string]any{
		"table_fragments": rowsPayload(c.queryRows(ctx, "oracle.storage.table_fragments", tableFragmentsQuery)),
	}
}

const tableFragmentsQuery = `
SELECT * FROM (
  SELECT owner AS "owner",
         table_name AS "table_name",
         ROUND(blocks * p.value / 1024 / 1024, 2) AS "table_size_mb",
         ROUND((avg_row_len * num_rows + ini_trans * 24) / NULLIF((blocks * p.value), 0) * 100, 2) AS "used_pct",
         ROUND(((blocks * p.value) - (avg_row_len * num_rows + ini_trans * 24)) / 1024 / 1024 * 0.9, 2) AS "safe_space_mb"
    FROM dba_tables t
    CROSS JOIN (SELECT value FROM v$parameter WHERE name = 'db_block_size') p
   WHERE blocks > 10240
   ORDER BY used_pct
) WHERE ROWNUM <= 20`
