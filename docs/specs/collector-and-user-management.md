# Collector management, users, and report ownership

Status: requirements settled 2026-10-01. Phase 1 (frontend on mock data) is ready for implementation; later phases are recorded here so the mock contract matches them.

Terms follow [`CONTEXT.md`](../../CONTEXT.md). Decisions: [ADR 0002](../adr/0002-collector-releases-published-by-ci.md) (CI-only releases), [ADR 0003](../adr/0003-persistent-platform-data.md) (persistent data, proposed).

## Problem

Engineers receive the collector by offline hand-off, so nobody knows which collector release is in the field. The web UI authenticates with one shared token, so report tasks have no submitter and history lives only in each browser. The platform must distribute collector releases and generate reports for signed-in users with two roles.

## Roles

| Capability | Engineer (普通用户) | Admin (管理员) |
|---|---|---|
| Download latest and deprecated releases | yes | yes |
| See and download pre-release and revoked releases | no | yes |
| Generate reports | yes | yes |
| See report tasks | own | all, filterable by submitter |
| Change release status | no | yes |
| Manage users (approve, reject, disable, enable, change role, reset password) | no | yes |
| See download records | no | yes |

## Accounts

Registration fields: username, display name, email, team, application note. Sign-in is local username and password. Phase 1 has no SSO and no email.

Account status transitions:

| From | Action | Actor | To | Notes |
|---|---|---|---|---|
| (none) | register | anyone | pending | |
| pending | approve | admin | active | |
| pending | reject | admin | rejected | reason required, shown to the user |
| rejected | resubmit application | user | pending | same username and email |
| active | disable | admin | disabled | reason required; submitted tasks are kept and stay visible to admins |
| disabled | enable | admin | active | |

What each status sees after sign-in: pending sees a waiting page; rejected sees the reason and a resubmit form; disabled is refused at sign-in with a message.

Admin rules:

- Admins can promote engineers to admin and demote admins.
- An admin cannot demote or disable themselves.
- The platform always keeps at least one active admin.
- The first admin comes from deployment config or an init command; nobody can register as admin.
- Password reset: an admin generates a temporary password and hands it over out of band. The user must change it at the next sign-in.
- Every account action records the acting admin and a timestamp.
- The "Users" nav item shows a badge with the pending count.

## Collector releases

Release statuses and transitions:

| From | Trigger | To | Notes |
|---|---|---|---|
| (none) | CI publishes tag `vX.Y.Z` | latest | the previous latest becomes deprecated |
| (none) | CI publishes tag `vX.Y.Z-rcN` | pre-release | |
| pre-release or deprecated | admin promotes | latest | the previous latest becomes deprecated; this is also the rollback path |
| latest, deprecated, or pre-release | admin deprecates | deprecated | |
| any except revoked | admin revokes | revoked | revocation reason required |
| revoked | admin restores | deprecated | |

Deprecating or revoking the latest release leaves the platform with no latest release until an admin promotes another one. This is assumed, not yet confirmed by the user.

Release metadata comes from CI and is read-only on the platform:

- version, git tag and commit, publish time
- release notes, generated from the tag annotation or `CHANGELOG.md`
- supported database types, which CI reads from the collector itself
- release packages: OS/arch, size, SHA256

Engineer collector page:

- The latest release at the top, with its release packages. The package matching the browser's platform is highlighted, and each SHA256 has a copy button.
- Usage instructions taken from the existing QUICKSTART content.
- Older releases collapsed underneath. Deprecated releases carry a warning tag; revoked releases are hidden.

Admin collector view:

- All releases in every status, with the status actions above.
- A release detail page with its download records, filterable by user and time.
- A global download records page.

Every package download creates a download record (user, release, package, time).

## Report tasks

- Each report task records its submitter. History is a server-side list; it no longer lives in the browser.
- Database types offered: mysql, oracle, gaussdb only.
- Retention: report task records are kept permanently. Uploaded ZIPs and generated reports expire after 30 days; expired tasks show "files expired".
- Each report item shows the `collector_version` read from its ZIP:
  - deprecated version: the report is generated and a notice is shown;
  - revoked version: the report is generated and a prominent warning shows the revocation reason;
  - an unknown version is shown as-is.
- The task list filters by status and time. Admins also get a submitter column and filter.
- The shared API token (`DBCHECK_API_TOKEN`) is retired; every request carries the user's identity. No scripts depend on it.

## Navigation

Routes:

- `/login`, `/register`, `/pending` (waiting page and rejection/resubmit)
- `/reports/new` (default after sign-in), `/reports`
- `/collectors`
- `/admin/users`, `/admin/downloads`

A shared layout shows the nav per role: Generate report / Report tasks / Collectors, and for admins also Users (with the pending badge) and Download records. A forced password change interrupts any route until it is completed. UI text is Chinese only, with no i18n framework.

## Phase 1: frontend on mock data

Scope: every screen above, running on mock data. No backend, CI, or runner changes.

- **API contract layer**: one typed module under `web/src/lib/api/` declares every operation (auth, users, releases, downloads, report tasks). A mock implementation and a real implementation share that contract. `NEXT_PUBLIC_API_MODE=mock|real` selects one at build time.
  - This replaces the current real-then-mock fallback in `generation-step.tsx` and the shared-token default in `lib/web-defaults.ts`.
- **Mock state** lives in `localStorage`, so one browser can register as an engineer, switch to an admin, and approve. A "reset mock data" action restores the seed.
- **Seed data**:
  - users: one admin, one active engineer, one pending, one rejected (with reason), one disabled;
  - releases: `1.3.0-rc1` pre-release, `1.2.0` latest, `1.1.0` deprecated, `1.0.0` revoked (with reason);
  - download records across those users;
  - report tasks from several submitters, including one with expired files and one item from a revoked collector version.
- The existing mocks (`lib/mock/mock-auth.ts`, `lib/mock/mock-tools.ts`, `lib/mock-api.ts`) and the tab store (`stores/nav-store.ts`) are replaced by the contract and the routes. The admin "publish release" form is removed (ADR 0002).

Done when every row of the role table, every account transition, and every release transition can be exercised in the browser in mock mode, and the web build and lint pass.

## Later phases (requirements only)

- **Backend**: users, sessions, release store, publish API with CI credential, download proxy and records, task submitter and list API, retention per ADR 0003.
- **CI**: GitHub Actions on `v*` tags, self-hosted intranet runner, reuses `scripts/build_release_packages.sh`, checks the tag against the collector version, publishes per ADR 0002. The collector gains a way to report its supported database types.
