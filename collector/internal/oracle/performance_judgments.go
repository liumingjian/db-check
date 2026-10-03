package oracle

import "context"

func (c *metricsCollector) collectPerformanceJudgments(ctx context.Context) map[string]any {
	return map[string]any{
		"latch_miss_ratios": rowsPayload(c.queryRows(ctx, "oracle.performance.latch_miss_ratios", latchMissRatiosQuery)),
		"time_model_ratios": rowsPayload(c.queryRows(ctx, "oracle.performance.time_model_ratios", timeModelRatiosQuery)),
	}
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
