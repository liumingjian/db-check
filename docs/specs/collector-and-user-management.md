# Collector management, users, and report ownership

Status: requirements settled 2026-10-01; layout and visual direction settled the same day by a UI prototype (see [UI](#ui)). Phase 1 (frontend on mock data) is ready for implementation; later phases are recorded here so the mock contract matches them.

Terms follow [`CONTEXT.md`](../../CONTEXT.md). Decisions: [ADR 0002](../adr/0002-collector-releases-published-by-ci.md) (CI-only releases), [ADR 0003](../adr/0003-persistent-platform-data.md) (persistent data, proposed).

## Problem

Engineers receive the collector by offline hand-off, so nobody knows which collector release is in the field. The web UI authenticates with one shared token, so report tasks have no submitter and history lives only in each browser. The platform must distribute collector releases and generate reports for signed-in users with two roles.

## Roles

| Capability | Engineer (普通用户) | Admin (管理员) |
|---|---|---|
| Download latest and deprecated releases | yes | yes |
| See and download pre-release and revoked releases | no | yes |
| Generate reports | yes | yes |
| See own report tasks in 我的报告 | yes | yes |
| See every user's report tasks in 全部报告, filterable by submitter | no | yes |
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
- The 管理 (Admin) nav item shows a badge with the pending count.

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

Deprecating or revoking the latest release leaves the platform with no latest release until an admin promotes another one. Meanwhile the collector page shows «暂无推荐版本，请联系管理员» in place of the latest release, and the older releases stay downloadable under their usual rules.

Release metadata comes from CI and is read-only on the platform:

- version, git tag and commit, publish time
- release notes, generated from the tag annotation or `CHANGELOG.md`
- supported database types, which CI reads from the collector itself
- release packages: OS/arch, size, SHA256

Release packages cover exactly four platforms: linux-amd64, linux-arm64, windows-amd64, windows-arm64. They are equals: customer core systems run Linux and Windows on both x86_64 and ARM64. No macOS package is built (`scripts/build_release_packages.sh`). Every release package is a `.zip`, on every platform, so the format is the same everywhere and older Windows Server releases without `tar` can unpack it. The packaging script writes them as `db-collector-<version>-<platform>.zip`, and the publish script (`scripts/publish_release.sh`) posts them.

Engineer collector page:

- The latest release at the top, with its four release packages shown as equal tiles. Nothing is highlighted by the browser's platform, because the collector runs on the customer's host, not the engineer's machine. Each SHA256 has a copy button.
- Usage instructions taken from the existing QUICKSTART content.
- Older releases collapsed underneath. Deprecated releases carry a warning tag; revoked releases are hidden.

Admin collector view:

- The same page, plus pre-release and revoked releases in the collapsed list. Status actions sit in a `···` menu on each release, together with 查看下载记录 (View download records), which opens 下载记录 filtered to that release.
- 管理 → 下载记录 (Download records) is the only download records page, filterable by user, release, and time. There is no release detail page.

Every package download creates a download record (user, release, package, time).

## Report tasks

- Each report task records its submitter. History is a server-side list; it no longer lives in the browser.
- Nobody picks a database type. When ZIPs are dropped, the browser reads each ZIP's `manifest.json` and shows one row per report item with its `db_type`.
  - A row whose manifest is unreadable, or whose `db_type` is not mysql, oracle, or gaussdb, is marked red and blocks submission.
  - An oracle row has an optional slot for one AWR HTML; a gaussdb row has an optional slot for several WDR HTMLs.
  - Dropped HTML files wait in a 待配对 (Unpaired) area until the user drags each onto a row.
- Retention: report task records are kept permanently. Uploaded ZIPs and generated reports expire after 30 days.
- Each report item keeps the `collector_version` read from its ZIP. Notices use the release's **current** status, so revoking a release later also flags reports already delivered:
  - deprecated version: the report is generated and a notice is shown while generating; report lists do not repeat it;
  - revoked version: the report is generated and a prominent warning shows the revocation reason, both while generating and in report lists;
  - an unknown version is shown as-is.
- 我的报告 (My reports) lists the signed-in user's own report tasks, admins included, newest first, with a one-click re-download and no filters. Users come here only when they missed or lost a download, so it stays a plain list. A row carries a small marker only when something is off: 生成中 (processing), 部分失败 or 失败 (expands to each item's reason), 已过期 (re-download disabled), or the yellow revoked warning with its reason. A plain successful row carries no marker.
- Admins see every user's report tasks under 管理 → 全部报告 (All reports), filterable by submitter.
- The shared API token (`DBCHECK_API_TOKEN`) is retired; every request carries the user's identity. No scripts depend on it.

## Navigation

Priority, by frequency and importance: 生成报告 (Generate report) first, then 采集器 (Collectors), then 我的报告 (My reports).

Routes:

- `/login`, `/register`, `/pending` (waiting page and rejection/resubmit)
- `/change-password`: every route redirects here while a forced password change is due; completing it returns to `/`. Active users also open it from the account menu (修改密码) to change their password voluntarily, which requires the current password
- `/` (default after sign-in): one long page with three sections in priority order, anchored `#new-report`, `#collectors`, `#reports`
- `/admin/users`, `/admin/downloads`, `/admin/reports`: the 管理 view, with tabs 用户 (Users) / 下载记录 / 全部报告

A slim fixed left rail carries the nav: the brand mark at the top, the section labels 生成报告 / 采集器 / 我的报告 set vertically in the middle (they scroll to their section; the current one is marked), 管理 for admins with the pending badge, and the account menu at the bottom (avatar with a 账号 caption; it holds 修改密码 and 退出登录). There is no top bar. UI text is Chinese only, with no i18n framework.

## UI

Chosen in a throwaway UI prototype: branch `worktree-ui-prototype-console`, verdict commit `3268e83`, variant D3b (`/prototype/console?variant=D3b`, dev only). Use it as the visual reference; do not merge it.

- Dark only. After ClickHouse's DESIGN.md on getdesign.md: canvas `#0a0a0a`, cards `#1a1a1a`, hairlines `#2a2a2a`, muted text `#888`, one accent, electric yellow `#faff69`, on primary actions, key numbers and full-bleed bands. Inter 700 with negative tracking for headlines, JetBrains Mono for commands and checksums.
- 生成报告: the first screen is the drop zone; after a drop, the per-item rows and the 待配对 area from [Report tasks](#report-tasks) appear beneath it. A large headline («拖进 ZIP，拿走报告。») with a select-files button; dropping anywhere on it adds ZIPs, and the whole screen turns yellow while a file is dragged over it. During generation one large percentage shows overall progress beside per-file rows. When done, a full-bleed yellow band («报告好了。») holds the download button.
- 采集器: four equal platform tiles (OS, architecture, size, download), SHA256 copy line, a three-step usage guide beside the command with a database switch, history collapsed.
- 我的报告: a plain hairline list (time, file name, re-download), plus the markers from [Report tasks](#report-tasks) on rows that need them.
- 管理: a separate view in the same language. The headline states the pending count; each applicant is a card with a yellow 批准 and a text 拒绝… link; lists use hairline rows with chip filters for people.
- `/pending`: same language. Pending shows a large waiting headline; rejected shows the reason and the resubmit form.
- `/change-password`: same language, a single form. A voluntary change adds a 当前密码 field and a 返回 link; a forced one offers only 退出登录.
- Motion follows Emil Kowalski's rules: nothing animates on frequent navigation; popovers and dialogs take 150–200 ms with `cubic-bezier(0.23, 1, 0.32, 1)` and scale from 0.95; buttons scale to 0.97 when pressed; transitions name their properties.

## Phase 1: frontend on mock data

Scope: every screen above, running on mock data. No backend, CI, or runner changes.

- **API contract layer**: one typed module under `web/src/lib/api/` declares every operation (auth, users, releases, downloads, report tasks). A mock implementation and a real implementation share that contract. `NEXT_PUBLIC_API_MODE=mock|real` selects one at build time.
  - This replaces the current real-then-mock fallback in `generation-step.tsx` and the shared-token default in `lib/web-defaults.ts`.
- **Mock state** lives in `localStorage`, so one browser can register as an engineer, switch to an admin, and approve. A "reset mock data" action restores the seed.
- **Seed data**:
  - users: one admin, one active engineer, one pending, one rejected (with reason), one disabled;
  - releases: `1.3.0-rc1` pre-release, `1.2.0` latest, `1.1.0` deprecated, `1.0.0` revoked (with reason);
  - download records across those users;
  - report tasks from several submitters, including one processing, one partly failed, one with expired files, and one item from a revoked collector version.
- **Seed fixture** (Phase 2): the seed lives in one JSON file, `tests/fixtures/console-seed.json`, read by the mock (`web/src/lib/api/seed-fixture.ts`) and later by the Go test server, so both start from identical data.
  - Top-level keys: `users` (with mock passwords), `releases` (with their packages), `downloadRecords`, `reportTasks`.
  - Every time is an offset from "now": `now`, or `now-` followed by days, hours, and minutes in that order, each optional (`now-1d2h3m`, `now-45m`). Readers resolve offsets against their clock when they seed.
  - The contract suites pin that clock to `2026-10-01T08:00:00Z` (`SEED_NOW` in `web/src/lib/api/testing.ts`), so seed records get fixed dates the suites can name.
- The existing mocks (`lib/mock/mock-auth.ts`, `lib/mock/mock-tools.ts`, `lib/mock-api.ts`) and the tab store (`stores/nav-store.ts`) are replaced by the contract and the routes. The admin "publish release" form is removed (ADR 0002).

Done when every row of the role table, every account transition, and every release transition can be exercised in the browser in mock mode, and the web build and lint pass.

## Later phases (requirements only)

- **Backend**: users, sessions, release store, publish API with CI credential, download proxy and records, task submitter and list API, retention per ADR 0003.
- **CI**: GitHub Actions on `v*` tags, self-hosted intranet runner, reuses `scripts/build_release_packages.sh` switched to `.zip` packages, checks the tag against the collector version, publishes per ADR 0002. The collector gains a way to report its supported database types.
