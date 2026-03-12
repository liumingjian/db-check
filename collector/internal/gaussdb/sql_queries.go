package gaussdb

type sqlQuerySpec struct {
	Name   string
	Domain string
	Label  string
	Query  string
}

var sqlQueryCatalog = [...]sqlQuerySpec{
	{
		Name:   "NoIndexSummary",
		Domain: "sql_analysis",
		Label:  "无索引表汇总",
		Query: `SELECT owner AS "owner",
       (SELECT COUNT(1) FROM db_tables) AS "total_table_count",
       COUNT(1) AS "no_index_count",
       ROUND(COUNT(1)::numeric / NULLIF((SELECT COUNT(1) FROM db_tables), 0) * 100, 4) AS "percentage"
  FROM db_tables
 WHERE table_name NOT IN (SELECT table_name FROM db_indexes)
   AND owner <> 'rdsAdmin'
 GROUP BY owner
 ORDER BY "no_index_count" DESC, owner`,
	},
	{
		Name:   "NoPrimaryKeySummary",
		Domain: "sql_analysis",
		Label:  "无主键表汇总",
		Query: `SELECT owner AS "owner",
       (SELECT COUNT(1) FROM db_tables) AS "total_table_count",
       COUNT(1) AS "no_pk_count",
       ROUND(COUNT(1)::numeric / NULLIF((SELECT COUNT(1) FROM db_tables), 0) * 100, 4) AS "percentage"
  FROM db_tables b
 WHERE table_name NOT IN (
           SELECT table_name
             FROM ADM_CONSTRAINTS
            WHERE constraint_type = 'p'
       )
   AND owner <> 'rdsAdmin'
 GROUP BY owner
 ORDER BY "no_pk_count" DESC, owner`,
	},
	{
		Name:   "NoPrimaryKeyDetail",
		Domain: "sql_analysis",
		Label:  "无主键表明细",
		Query: `SELECT owner AS "owner",
       table_name AS "table_name"
  FROM db_tables b
 WHERE table_name NOT IN (
           SELECT table_name
             FROM ADM_CONSTRAINTS
            WHERE constraint_type = 'p'
       )
   AND owner <> 'rdsAdmin'
 ORDER BY owner, table_name`,
	},
	{
		Name:   "NoStatisticsSummary",
		Domain: "sql_analysis",
		Label:  "统计信息缺失汇总",
		Query: `SELECT tableowner AS "tableowner",
       (SELECT COUNT(1) FROM PG_tables) AS "total_table_count",
       COUNT(1) AS "table_no_stat",
       ROUND(COUNT(1)::numeric / NULLIF((SELECT COUNT(1) FROM PG_tables), 0) * 100, 4) AS "percentage"
  FROM PG_tables
 WHERE tablename NOT IN (SELECT relname FROM GS_TABLESTATS_HISTORY)
   AND schemaname NOT IN ('pg_catalog')
 GROUP BY tableowner
 ORDER BY "table_no_stat" DESC, tableowner`,
	},
	{
		Name:   "NoStatisticsDetail",
		Domain: "sql_analysis",
		Label:  "统计信息缺失明细",
		Query: `SELECT schemaname AS "schemaname",
       tableowner AS "tableowner",
       tablename AS "tablename"
  FROM PG_tables
 WHERE tablename NOT IN (SELECT relname FROM GS_TABLESTATS_HISTORY)
   AND schemaname NOT IN ('pg_catalog')
 ORDER BY schemaname, tableowner, tablename`,
	},
}
