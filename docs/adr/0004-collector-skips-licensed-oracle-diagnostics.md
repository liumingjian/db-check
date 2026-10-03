---
status: accepted
---

# The collector never reads licensed Oracle diagnostic data

Oracle's Diagnostics Pack (AWR history, ASH, ADDM) needs an extra licence, and querying its views on a customer database can create a licence-compliance exposure for the customer. The collector therefore never queries `dba_hist_*`, ASH, or ADDM views, and ships no switch that enables them. Deeper diagnosis from that data comes only from an AWR report the engineer uploads with the report item, which the reporter already accepts.

If a future release adds an opt-in collection switch, it must be explicit per run and the report must state the licence precondition. That change needs a new ADR superseding this one.

## Considered Options

- **Query AWR views by default**: rejected. It gives richer evidence, but the collector runs on customer hosts, and a licence audit finding would land on the customer and on us.
- **Opt-in switch now**: rejected for this release. No current check needs it, and an unused switch invites silent use on unlicensed databases.
