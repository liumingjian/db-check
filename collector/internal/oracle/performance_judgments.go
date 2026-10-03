package oracle

import "context"

func (c *metricsCollector) collectPerformanceJudgments(ctx context.Context) map[string]any {
	return map[string]any{
		"latch_miss_ratios": rowsPayload(c.queryPerformanceRows(ctx, "oracle.performance.latch_miss_ratios", latchMissRatiosQuery, "miss_pct")),
		"time_model_ratios": rowsPayload(c.queryPerformanceRows(ctx, "oracle.performance.time_model_ratios", timeModelRatiosQuery, "parse_pct")),
	}
}

func (c *metricsCollector) queryPerformanceRows(ctx context.Context, scope, query, field string) []map[string]any {
	rows := c.queryRows(ctx, scope, query)
	if evidence, ok := c.availability[datasetPath(scope)].(map[string]any); ok && evidence["readable"] == false {
		return rows
	}
	for _, row := range rows {
		if row[field] != nil {
			return rows
		}
	}
	c.markUnavailable(scope, "缺少有效性能样本或可用分母，无法判定正常", "请 DBA 确认有业务活动、有效计时与正数资源上限，并在业务时段重新采集；UNLIMITED 上限不计算容量比例。")
	return rows
}

const latchMissRatiosQuery = `
SELECT MAX(CASE WHEN gets > 0 THEN misses/gets*100 END) AS "miss_pct"
 FROM v$latch`

// Use unrounded values from the complete time model, independent of report TopN.
const timeModelRatiosQuery = `
SELECT CASE WHEN MAX(CASE WHEN stat_name='DB time' THEN value END) > 0 THEN
         MAX(CASE WHEN stat_name='parse time elapsed' THEN value END) /
         MAX(CASE WHEN stat_name='DB time' THEN value END)*100
       END AS "parse_pct"
 FROM v$sys_time_model`
