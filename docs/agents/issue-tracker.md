# Issue tracker: GitHub

Issues and specs live in GitHub Issues for `liumingjian/db-check`.
Use the `gh` CLI from this repository.

## Conventions

- Create: `gh issue create --title "..." --body-file <path>`.
- Read: `gh issue view <number> --json number,title,body,labels,comments`.
- List: `gh issue list --state open --json number,title,body,labels,comments`.
  Apply label and state filters as needed.
- Comment: `gh issue comment <number> --body-file <path>`.
- Label: `gh issue edit <number> --add-label "..."` or
  `gh issue edit <number> --remove-label "..."`.
- Close: `gh issue close <number>`.

For multiline bodies, write the exact Markdown to a temporary file and
pass it with `--body-file`. When operating outside the checkout, specify
`--repo liumingjian/db-check`.

When a skill says "publish to the issue tracker", create a GitHub issue.
When it says "fetch the relevant ticket", read the issue and its comments.

## Pull requests as a triage surface

**PRs as a request surface: no.**

If enabled later, use `gh pr view`, `gh pr diff`, `gh pr list`,
`gh pr comment`, `gh pr edit`, and `gh pr close`. Include external
authors with association CONTRIBUTOR, FIRST_TIME_CONTRIBUTOR, or NONE.

GitHub shares issue and PR numbers. For an ambiguous reference, try
`gh pr view <number>` and fall back to `gh issue view <number>`.

## Wayfinding operations

Used by `/wayfinder`.

- Map: one issue labelled `wayfinder:map`, containing Notes,
  Decisions-so-far, and Fog.
- Child ticket: link it as a GitHub sub-issue using `gh api`.
  If sub-issues are unavailable, add it to the map's task list and put
  `Part of #<map>` at the top of its body.
- Type: label children `wayfinder:<type>`, where type is research,
  prototype, grilling, or task.
- Blocking: prefer native issue dependencies. Add a blocker with
  `gh api --method POST repos/liumingjian/db-check/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`.
  Obtain the database ID with
  `gh api repos/liumingjian/db-check/issues/<blocker> --jq .id`.
  If dependencies are unavailable, use `Blocked by: #<n>, #<n>` in
  the child body.
- Frontier: inspect the map's open children in map order. Choose the
  first unassigned child with no open blockers. Check native
  `issue_dependencies_summary.blocked_by` or the fallback references.
- Claim: assign the ticket with `gh issue edit <number> --add-assignee @me`
  before starting work.
- Resolve: comment with the answer, close the ticket, and append a
  summary and link to the map's Decisions-so-far.
