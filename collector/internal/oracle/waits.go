package oracle

import (
	"context"
	"fmt"
)

func (c *metricsCollector) collectWaitMetrics(ctx context.Context) map[string]any {
	return map[string]any{
		"wait_events": rowsPayload(c.queryRows(ctx, "oracle.performance.wait_events", waitEventsQuery(c.cfg.TopN))),
		"latch_data":  rowsPayload(c.queryRows(ctx, "oracle.performance.latch_data", latchDataQuery(c.cfg.TopN))),
		"time_model":  rowsPayload(c.queryRows(ctx, "oracle.performance.time_model", timeModelQuery(c.cfg.TopN))),
	}
}

func waitEventsQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT event AS "event",
         total_waits AS "waits",
         ROUND(time_waited_micro/1000, 2) AS "waited_ms",
         ROUND(average_wait/100, 2) AS "avg_wait_ms"
    FROM v$system_event
   WHERE wait_class NOT IN ('Idle')
   ORDER BY time_waited_micro DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

func latchDataQuery(topN int) string {
	return `
SELECT * FROM (
  SELECT name AS "name",
         gets AS "gets",
         misses AS "misses",
         sleeps AS "sleeps",
         immediate_gets AS "immediate_gets",
         immediate_misses AS "immediate_misses",
         spin_gets AS "spin_gets"
    FROM v$latch
   WHERE misses > 0
   ORDER BY misses DESC
) WHERE ROWNUM <= ` + formatTopN(topN)
}

func timeModelQuery(topN int) string {
	return fmt.Sprintf(`
SELECT * FROM (
  SELECT stat_name AS "stat_name",
         ROUND(value/1000000, 2) AS "seconds"
    FROM v$sys_time_model
   WHERE value > 0
   ORDER BY value DESC
) WHERE ROWNUM <= %s`, formatTopN(topN))
}
