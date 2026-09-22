# Database report generation

This context covers generating database reports from submitted diagnostic inputs.

## Language

**Report task**:
A submitted batch of report items with a collective outcome and, when report generation succeeds, a downloadable collection of reports. A task can contain both successful and failed items.
_Avoid_: Report item when referring to the whole submission.

**Report item**:
One report-generation unit consisting of a primary diagnostic ZIP and its paired optional AWR or WDR inputs. Each item has its own outcome and can produce one report document.
_Avoid_: Report task when referring to one unit within a submission.
