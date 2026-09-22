# Domain docs

## Layout and reading rules

This repository uses a single-context layout:

- `CONTEXT.md` at the repository root: domain vocabulary and model.
- `docs/adr/`: architecture decision records.

Before exploring the codebase, read `CONTEXT.md` and the ADRs relevant
to the area being explored.

If these files are absent, proceed silently. Create them through
`/domain-modeling` when domain terms or decisions are resolved.

## Vocabulary

Use terms defined in `CONTEXT.md` when naming domain concepts in
issues, proposals, hypotheses, code, and tests.

If a needed concept is absent, reconsider whether the term fits the
project. Note genuine glossary gaps for `/domain-modeling`.

## Decision conflicts

Explicitly flag any proposal that contradicts an existing ADR.
Identify the ADR and explain why its decision should be reconsidered.
