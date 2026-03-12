package oracle

import "context"

func (c *metricsCollector) collectStorage(ctx context.Context) map[string]any {
	payload := map[string]any{
		"datafile_total_gb": c.queryInt64(
			ctx,
			"oracle.storage.datafile_total_gb",
			`SELECT CEIL(SUM(bytes)/1024/1024/1024) AS "datafile_total_gb" FROM v$datafile`,
		),
		"tablespace_count": c.queryInt64(ctx, "oracle.storage.tablespace_count", `SELECT COUNT(1) AS "tablespace_count" FROM dba_tablespaces`),
		"datafile_count":   c.queryInt64(ctx, "oracle.storage.datafile_count", `SELECT COUNT(1) AS "datafile_count" FROM dba_data_files`),
		"controlfile_count": c.queryInt64(
			ctx,
			"oracle.storage.controlfile_count",
			`SELECT COUNT(1) AS "controlfile_count" FROM v$controlfile`,
		),
		"redo_size_mb": c.queryInt64(ctx, "oracle.storage.redo_size_mb", `SELECT ROUND(bytes/1024/1024) AS "redo_size_mb" FROM v$log WHERE rownum < 2`),
		"redo_group_count": c.queryInt64(
			ctx,
			"oracle.storage.redo_group_count",
			`SELECT COUNT(group#) AS "redo_group_count" FROM gv$log`,
		),
		"control_files": rowsPayload(c.queryRows(ctx, "oracle.storage.control_files", `SELECT name AS "name" FROM v$controlfile`)),
		"redo_logs": rowsPayload(c.queryRows(
			ctx,
			"oracle.storage.redo_logs",
			`SELECT group# AS "group_id", thread# AS "thread_id", sequence# AS "sequence", bytes AS "bytes", members AS "members", archived AS "archived", status AS "status", first_time AS "first_time" FROM v$log`,
		)),
		"datafiles": rowsPayload(c.queryRows(
			ctx,
			"oracle.storage.datafiles",
			`SELECT file_name AS "file_name", tablespace_name AS "tablespace_name", status AS "status", ROUND(bytes/1024/1024/1024,2) AS "current_gb", autoextensible AS "autoextensible", ROUND(maxbytes/1024/1024/1024,2) AS "max_gb" FROM dba_data_files ORDER BY file_name`,
		)),
		"recover_files": rowsPayload(c.queryRows(
			ctx,
			"oracle.storage.recover_files",
			`SELECT file# AS "file_id", online_status AS "online_status", error AS "error", change# AS "change_number", time AS "time" FROM v$recover_file`,
		)),
		"tablespace_usage": rowsPayload(c.queryRows(ctx, "oracle.storage.tablespace_usage", tablespaceUsageQuery)),
		"invalid_objects": rowsPayload(c.queryRows(
			ctx,
			"oracle.storage.invalid_objects",
			`SELECT owner AS "owner", object_type AS "object_type", COUNT(*) AS "object_count" FROM dba_objects WHERE status='INVALID' GROUP BY owner, object_type ORDER BY owner, object_type`,
		)),
		"invalid_indexes": rowsPayload(c.queryRows(ctx, "oracle.storage.invalid_indexes", invalidIndexesQuery)),
	}
	return mergeMaps(payload, c.collectStorageExtra(ctx))
}

const tablespaceUsageQuery = `
SELECT d.tablespace_name AS "tablespace_name",
       d.max_space AS "max_size_gb",
       d.space AS "total_size_gb",
       d.space - NVL(f.free_space, 0) AS "used_size_gb",
       ROUND(((d.space - NVL(f.free_space, 0)) / CASE WHEN d.max_space = 0 THEN 1 ELSE d.max_space END) * 100, 2) AS "real_percent"
  FROM (
        SELECT tablespace_name,
               SUM(max_space) AS max_space,
               SUM(space) AS space
          FROM (
                SELECT tablespace_name,
                       ROUND(DECODE(autoextensible, 'YES', SUM(maxbytes)/(1024*1024*1024), SUM(bytes)/(1024*1024*1024)), 2) AS max_space,
                       ROUND(SUM(bytes)/(1024*1024*1024), 2) AS space
                  FROM dba_data_files
                 GROUP BY tablespace_name, autoextensible
               )
         GROUP BY tablespace_name
       ) d
  LEFT JOIN (
        SELECT tablespace_name,
               ROUND(SUM(bytes)/(1024*1024*1024), 2) AS free_space
          FROM dba_free_space
         GROUP BY tablespace_name
       ) f
    ON d.tablespace_name = f.tablespace_name
 ORDER BY d.tablespace_name`

const invalidIndexesQuery = `
SELECT owner AS "owner", index_name AS "index_name", '' AS "subname", status AS "status" FROM dba_indexes WHERE status = 'UNUSABLE'
UNION
SELECT index_owner AS "owner", index_name AS "index_name", partition_name AS "subname", status AS "status" FROM dba_ind_partitions WHERE status = 'UNUSABLE'
UNION
SELECT index_owner AS "owner", index_name AS "index_name", subpartition_name AS "subname", status AS "status" FROM dba_ind_subpartitions WHERE status = 'UNUSABLE'`
