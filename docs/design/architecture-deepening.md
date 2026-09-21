# Architecture deepening working note

This note records design direction. It does not authorize implementation.

## Accepted direction

- Address web task lifecycle, HTML table extraction, and outer report assembly.
- The overload choice alone does not need an ADR. The accepted single-writer storage scope is recorded in [ADR 0001](../adr/0001-web-task-lifecycle-ownership.md).
- Do not add generic programming terms to `CONTEXT.md`.

## Web task lifecycle

The [web task lifecycle design](web-task-lifecycle.md) is the source of truth for the accepted behavior, code evidence, proposed module interface, and acceptance scenarios. The user accepted discussion decisions Q1 through Q13. Publication mechanics, the interface sketch, and legacy-client idempotency compatibility remain subject to final design review. Implementation is not authorized.

## HTML table extraction

No parser compatibility decisions have been accepted.

- Which AWR and WDR results, errors, unit parsing, optional lookups, and match behavior must remain?
- Which tests define required parser behavior before extraction code is shared?

## Outer report assembly

No report compatibility decisions have been accepted.

- Which `ReportView` and `SummaryStrategy` behavior must remain?
- Which title, time, document-control, section-order, summary, and detail behavior must remain database-specific?

No implementation is authorized by this note.
