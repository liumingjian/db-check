package oracle

import (
	"context"
	"fmt"
)

func (c *metricsCollector) collectStorageExtra(ctx context.Context) map[string]any {
	payload := map[string]any{
		"table_fragments":    rowsPayload(c.queryRows(ctx, "oracle.storage.table_fragments", tableFragmentsQuery)),
		"temp_usage":         rowsPayload(c.queryRows(ctx, "oracle.storage.temp_usage", tempUsageQuery)),
		"sysaux_usage":       rowsPayload(c.queryRows(ctx, "oracle.storage.sysaux_usage", `SELECT * FROM (`+tablespaceUsageQuery+`) WHERE "tablespace_name"='SYSAUX'`)),
		"largest_segments":   rowsPayload(c.queryRows(ctx, "oracle.storage.largest_segments", largestSegmentsQuery)),
		"recyclebin_size_gb": c.queryFloat64(ctx, "oracle.storage.recyclebin_size_gb", `SELECT NVL(SUM(r.space*t.block_size),0)/POWER(1024,3) FROM dba_recyclebin r JOIN dba_tablespaces t ON t.tablespace_name=r.ts_name`),
	}
	for _, item := range []struct{ source, target, field string }{
		{"temp_usage", "temp_usage_max_pct", "real_percent"},
		{"sysaux_usage", "sysaux_usage_pct", "real_percent"},
		{"largest_segments", "largest_segment_usage_pct", "tablespace_percent"},
	} {
		rows := payload[item.source].(map[string]any)["items"].([]map[string]any)
		var maximum any = float64(0)
		valid := len(rows) > 0 || item.source == "largest_segments"
		for _, row := range rows {
			var value float64
			if row[item.field] == nil {
				valid = false
				continue
			}
			if _, err := fmt.Sscan(fmt.Sprint(row[item.field]), &value); err != nil {
				valid = false
				continue
			}
			if value > maximum.(float64) {
				maximum = value
			}
		}
		if !valid {
			maximum = nil
		}
		payload[item.target] = maximum
		source := "db.storage." + item.source
		if evidence, ok := c.availability[source]; ok {
			c.availability["db.storage."+item.target] = evidence
			if record, ok := evidence.(map[string]any); ok && record["readable"] == true && !valid {
				c.markUnavailable("oracle.storage."+item.target, "缺少可用容量或使用率数据，无法判定空间状态", "请 DBA 在当前容器确认表空间和文件容量，检查采集明细后重新运行 Collector。")
			}
		}
	}
	return payload
}

const tempUsageQuery = `
SELECT d.tablespace_name AS "tablespace_name", t.block_size AS "block_size",
       ROUND(d.total_bytes/POWER(1024,3),2) AS "total_size_gb",
       ROUND(d.max_bytes/POWER(1024,3),2) AS "max_size_gb",
       ROUND(NVL(u.blocks,0)*t.block_size/POWER(1024,3),2) AS "used_size_gb",
       NVL(u.blocks,0)*t.block_size/NULLIF(d.max_bytes,0)*100 AS "real_percent"
 FROM (SELECT tablespace_name, SUM(bytes) AS total_bytes,
              SUM(CASE WHEN autoextensible='YES' THEN GREATEST(bytes,NVL(maxbytes,bytes)) ELSE bytes END) AS max_bytes
         FROM dba_temp_files GROUP BY tablespace_name) d
 JOIN dba_tablespaces t ON t.tablespace_name=d.tablespace_name
 LEFT JOIN (SELECT tablespace, SUM(blocks) AS blocks FROM gv$tempseg_usage GROUP BY tablespace) u ON u.tablespace=d.tablespace_name
 ORDER BY d.tablespace_name`

const largestSegmentsQuery = `
SELECT * FROM (
 SELECT s.owner AS "owner", s.segment_name AS "segment_name", s.partition_name AS "partition_name",
        s.segment_type AS "segment_type", s.tablespace_name AS "tablespace_name",
        ROUND(s.bytes/POWER(1024,3),2) AS "size_gb", s.bytes/NULLIF(d.max_bytes,0)*100 AS "tablespace_percent"
 FROM dba_segments s JOIN (
  SELECT tablespace_name, SUM(CASE WHEN autoextensible='YES' THEN GREATEST(bytes,NVL(maxbytes,bytes)) ELSE bytes END) AS max_bytes
  FROM dba_data_files GROUP BY tablespace_name
 ) d ON d.tablespace_name=s.tablespace_name
 ORDER BY s.bytes DESC, s.owner, s.segment_name, s.partition_name
) WHERE ROWNUM <= 20`

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
