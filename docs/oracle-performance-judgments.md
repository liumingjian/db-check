# Oracle performance judgments

Checks 4.17 through 4.23 judge previously informational datasets. They preserve the original 27 identifiers.

| Check | Metric | Warning | Critical |
| --- | --- | --- | --- |
| 4.17 | Maximum average non-idle wait within collected TopN events, milliseconds | >10 | >50 |
| 4.18 | Maximum willing-to-wait latch misses/gets, percent | >1 | >5 |
| 4.19 | Maximum current use/finite positive limit across collected resources and instances, percent | >80 | >90 |
| 4.20 | Parse time elapsed/DB time, percent | >10 | >20 |
| 4.21 | ORA-01555 plus no-space errors in collected recent UNDO intervals | None | >0 |
| 4.22 | Tables configured for non-serial degree, including DEFAULT | Any row | None |
| 4.23 | Indexes configured for non-serial degree, including DEFAULT | Any row | None |

The latency, latch and parsing thresholds are project investigation baselines. The resource thresholds reuse the existing capacity baseline. Neither reviewed reference supplies thresholds for these same metrics and units. Any recorded UNDO error warrants investigation; parallel degree is an advisory configuration review rather than proof of a performance fault. See [reference comparison](oracle-threshold-reference.md) for the pinned sources and verified comparisons. IDs 4.11–4.16 remain reserved for uploaded AWR checks; the Collector does not read licensed diagnostic data.

## Time and scope

Wait events, latch requests and the time model are cumulative since instance startup. The Collector takes one observation; it does not calculate an interval delta. A finding therefore identifies a historical ratio or average that needs follow-up during the relevant workload. Wait and latch views are for the connected instance; resource limits cover instances through `GV$RESOURCE_LIMIT`. When connected to CDB root, current-state performance views can represent instance-wide activity rather than an individual PDB workload. No licensed diagnostics are queried.

The average wait column uses Oracle's hundredths-of-a-second unit multiplied by ten for milliseconds. Non-idle wait events are ranked by total waited time and limited to TopN, so the maximum latency check is only over that displayed set. Time model ratios read the complete underlying view, using raw microseconds independently of the displayed TopN rows. Time model values can lag active timed operations slightly; they are not a synchronized interval measurement.

Resource values use numeric, positive finite limits. `UNLIMITED`, zero, missing, and malformed limits yield no percentage. Missing or zero DB time and a lack of latch requests likewise yield unknown ratios, never a fabricated zero. If no usable numeric sample exists, the Collector records unavailable evidence so the check is not collected. Failed view reads preserve the original Oracle error and become insufficient privilege or not collected. Within a mixed resource set, the maximum evaluates finite resources; individual unknown limits remain visible in the details.

UNDO intervals are the most recent TopN rows of `V$UNDOSTAT`, not a cumulative lifetime counter or a guaranteed seven-day window. DEGREE is trimmed before comparing it to serial settings, so whitespace around `1` does not create a false finding. `DEFAULT` can request automatically chosen parallelism and is included for review.

## Oracle references

- [V$SYSTEM_EVENT](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-SYSTEM_EVENT.html) documents average wait units and timing availability. Disabled timing can prevent a useful latency measurement; consult the DBA when timing evidence is absent.
- [V$LATCH](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-LATCH.html) defines GETS and MISSES as willing-to-wait requests.
- [V$RESOURCE_LIMIT](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-RESOURCE_LIMIT.html) defines current utilization, lifetime peak and textual limits.
- [V$SYS_TIME_MODEL](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-SYS_TIME_MODEL.html) defines accumulated microseconds and timing publication delay.

Runtime verification must use the remote Mac executor designated for issue #48. Formatting and source inspection do not imply a passing fixture suite or Oracle smoke run.
