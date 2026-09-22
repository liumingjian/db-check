# Architecture deepening working note

This note records design direction. It does not authorize implementation.

## Accepted direction

- Address web task lifecycle, HTML table extraction, and outer report assembly.
- The overload choice alone does not need an ADR. The accepted single-writer storage scope is recorded in [ADR 0001](../adr/0001-web-task-lifecycle-ownership.md).
- Do not add generic programming terms to `CONTEXT.md`.

## Web task lifecycle

The [web task lifecycle design](web-task-lifecycle.md) is the source of truth for the accepted behavior, code evidence, proposed module interface, and acceptance scenarios. The user accepted discussion decisions Q1 through Q13. Publication mechanics, the interface sketch, and legacy-client idempotency compatibility remain subject to final design review. Implementation is not authorized.

## HTML table extraction

The accepted design goal is to reduce duplicate maintenance while preserving current parser behavior.

- Preserve existing parser results, exception types, match behavior and order, and units.
- Decide bugs separately from extraction design. This decision does not establish bug compatibility requirements.

The shared layer will contain the following:

- The HTML table collector and a common table data structure.
- Text and whitespace normalization.
- Row mapping.
- Ordinary numeric parsing with existing semantics.

AWR and WDR will retain the following:

- Required and optional selection.
- First-match and all-match policy.
- Units and report field semantics.
- Their own exceptions and validations.

Small duplicated validation code is accepted. The shared layer needs no error adaptation. Do not add a generic exception factory or error translation.

### Compatibility acceptance criteria

- For every existing AWR and WDR fixture, the full parsed result is equal before and after extraction.
- For a missing required table, the exception type and message remain unchanged.
- Match ordering, the handling of absent optional tables, and unit parsing remain unchanged.
- Direct tests for shared helpers cover whitespace normalization, nested formatting tags inside cells but not nested tables, uneven row lengths, and ordinary numeric parsing.

### Suspected issues

- A nested table may cause the parser to lose the outer table.
- Normalized duplicate headers can overwrite earlier columns.
- The parser accepts `NaN` and `Infinity`.

The extraction must preserve behavior before and after the change. Fixes require separate decisions, and these observations do not create permanent support guarantees. Implementation remains unauthorized.

## Outer report assembly

No report compatibility decisions have been accepted.

- Which `ReportView` and `SummaryStrategy` behavior must remain?
- Which title, time, document-control, section-order, summary, and detail behavior must remain database-specific?

No implementation is authorized by this note.
