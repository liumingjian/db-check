# Oracle space and alert checks

Permanent tablespace usage divides allocated bytes minus free bytes by the sum of each file's maximum autoextend capacity. A non-autoextending file contributes its current capacity. The maximum is never less than current capacity. Aggregate files and free extents separately, then left join them so fully used tablespaces remain visible. Percentages use unrounded bytes.

Temporary space reports active temporary segments across instances from `GV$TEMPSEG_USAGE`, multiplied by the tablespace's own `DBA_TABLESPACES.BLOCK_SIZE`. On 12c and later it filters `CON_ID` to the connected container before grouping, matching the local `DBA_TEMP_FILES` capacity. Capacity includes autoextend limits. Cached reusable allocations are not active usage. No active segments means zero use; an unreadable source means a coverage gap.

SYSAUX uses the same capacity calculation. Largest segments list the twenty largest physical segments in the connected container, including partitions. Their capacity percentage uses the owning tablespace's maximum capacity. The rule evaluates the maximum percentage within that displayed list, rather than estimating growth. Recycle bin size uses each object's block count and its tablespace's block size; it is an estimate of reclaimable allocation, not a recommendation to purge without checking flashback requirements.

## Threshold provenance

Existing tablespace thresholds remain warning above 80% and critical above 90%. Temporary space and SYSAUX reuse those capacity thresholds as required by the spec. [DBCheck's Oracle rules](https://github.com/fiyo/DBCheck/blob/8c0de837141723abed7d094fb41839e611871b1e/modules/pro/rules/builtin/oracle.yaml) identify temporary space above 90% as high risk and SYSAUX above 85% as medium risk. The project SYSAUX warning threshold of 80% is more conservative.

The largest-segment capacity rule reuses the existing 80/90 capacity thresholds as a concentration warning. Any positive recycle bin allocation produces an advisory warning. These two choices are project policies; neither reviewed reference supplies a threshold for the same metric.

## Alert log coverage

On version 12 or later the Collector reads `V$DIAG_ALERT_EXT` for the last seven days, filters records containing `ORA-` followed by five digits, orders by timestamp and record ID descending, and takes the latest 200 matching records. Every ORA code in each message is extracted; repetitions of a code within one record count once. Each code retains its matching-record count and latest message and timestamp. Counts do not represent all errors in seven days when the 200-record limit is reached.

The Analyzer's declarative rules classify ORA-00600, ORA-07445, ORA-01555 and ORA-04031 as critical and other codes as warnings. The Collector retains raw codes and matching-record counts without severity. On 11g or when version probing fails, alert collection is explicitly unavailable. A DBA should inspect the original alert log or use ADRCI `SHOW ALERT` for the seven-day window. Failed reads and missing grants remain coverage gaps, including derived space metrics.

SQL execution and end-to-end verification require the remote Mac executor specified by the parent issue. No database smoke result is implied by the SQL review or fixture tests.
