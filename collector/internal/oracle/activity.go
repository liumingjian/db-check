package oracle

import "context"

func (c *metricsCollector) collectSessionActivity(ctx context.Context) map[string]any {
	return map[string]any{
		"active_session_details": rowsPayload(c.queryRows(ctx, "oracle.performance.active_session_details", activeSessionDetailsQuery)),
		"long_transactions":      rowsPayload(c.queryRows(ctx, "oracle.performance.long_transactions", longTransactionsQuery)),
		"blocking_chains":        rowsPayload(c.queryRows(ctx, "oracle.performance.blocking_chains", blockingChainsQuery)),
	}
}

const activeSessionDetailsQuery = `
SELECT sid AS "sid",
       serial# AS "serial",
       username AS "username",
       machine AS "machine",
       program AS "program",
       sql_id AS "sql_id",
       event AS "event",
       state AS "state",
       seconds_in_wait AS "seconds_in_wait"
  FROM v$session
 WHERE status = 'ACTIVE'
   AND type != 'BACKGROUND'
   AND wait_class != 'Idle'`

const longTransactionsQuery = `
SELECT s.sid AS "sid",
       s.serial# AS "serial",
       s.username AS "username",
       s.program AS "program",
       s.status AS "session_status",
       s.sql_id AS "sql_id",
       s.event AS "event",
       TO_CHAR(t.start_date, 'yyyy-mm-dd hh24:mi:ss') AS "start_time",
       ROUND((SYSDATE - t.start_date) * 24 * 60, 2) AS "duration_minutes",
       t.status AS "transaction_status"
  FROM v$session s
  JOIN v$transaction t
    ON s.taddr = t.addr`

const blockingChainsQuery = `
SELECT waiter.sid AS "waiter_sid",
       waiter.serial# AS "waiter_serial",
       waiter.username AS "waiter_username",
       blocker.sid AS "blocker_sid",
       blocker.serial# AS "blocker_serial",
       blocker.username AS "blocker_username",
       waiter.event AS "wait_event",
       waiter.seconds_in_wait AS "seconds_in_wait"
  FROM v$session waiter
  JOIN v$session blocker
    ON waiter.blocking_session = blocker.sid
 WHERE waiter.blocking_session IS NOT NULL`
