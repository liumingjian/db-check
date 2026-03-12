package oracle

import "context"

func (c *metricsCollector) collectBasicInfo(ctx context.Context) map[string]any {
	return map[string]any{
		"db_name":       c.queryString(ctx, "oracle.basic.db_name", `SELECT name AS "db_name" FROM v$database`),
		"instance_name": c.queryString(ctx, "oracle.basic.instance_name", `SELECT instance_name AS "instance_name" FROM v$instance`),
		"dbid":          c.queryInt64(ctx, "oracle.basic.dbid", `SELECT dbid AS "dbid" FROM v$database`),
		"is_rac": yesNoToBool(c.queryString(
			ctx,
			"oracle.basic.is_rac",
			`SELECT decode(value,'TRUE','Yes','No') AS "is_rac" FROM v$option WHERE parameter='Real Application Clusters'`,
		)),
		"version": c.queryString(
			ctx,
			"oracle.basic.version",
			`SELECT version AS "version" FROM product_component_version WHERE rownum < 2`,
		),
		"character_set": c.queryString(
			ctx,
			"oracle.basic.character_set",
			`SELECT value AS "character_set" FROM nls_database_parameters WHERE parameter='NLS_CHARACTERSET'`,
		),
		"log_mode": c.queryString(ctx, "oracle.basic.log_mode", `SELECT log_mode AS "log_mode" FROM v$database`),
		"alert_log": c.queryString(
			ctx,
			"oracle.basic.alert_log",
			`SELECT value AS "alert_log" FROM v$parameter WHERE name='background_dump_dest'`,
		),
	}
}

func (c *metricsCollector) collectConfigCheck(ctx context.Context) map[string]any {
	return map[string]any{
		"spfile": c.queryString(
			ctx,
			"oracle.config.spfile",
			`SELECT value AS "spfile" FROM gv$parameter WHERE name='spfile' AND inst_id=1`,
		),
		"sga_target_mb": c.queryString(
			ctx,
			"oracle.config.sga_target_mb",
			`SELECT ROUND(value/1024/1024, 2) AS "sga_target_mb" FROM v$parameter WHERE name='sga_target'`,
		),
		"db_block_size_kb": c.queryString(
			ctx,
			"oracle.config.db_block_size_kb",
			`SELECT ROUND(value/1024, 2) AS "db_block_size_kb" FROM v$parameter WHERE name='db_block_size'`,
		),
		"parameters": rowsPayload(c.queryRows(
			ctx,
			"oracle.config.parameters",
			`SELECT name AS "name", value AS "value" FROM v$spparameter WHERE value IS NOT NULL`,
		)),
	}
}
