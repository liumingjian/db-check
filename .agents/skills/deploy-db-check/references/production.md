# Production deployment

## Target and scope

The configured target is `root@10.250.0.222`, directory `/opt/tools/db-check`.
The console is `http://10.250.0.222:3000`; the API is
`http://10.250.0.222:18080`. Obtain SSH credentials from the current user/session.
This runbook intentionally contains no password.

The initial host inspection on 2026-10-08 found Kylin V10 x86_64, Node 20.19.5,
Go 1.25.5, Python 3.13.7, and PM2. Verify the available versions on later runs.
The previous `dbcheck-api` and `dbcheck-web` used
`/root/lmj/projects/db-check`, ports 18080 and 3000, and `/tmp/dbcheck-data`.
That initial data directory was empty and had no `platform.db`. Inspect the
active API definition for subsequent deployments; its data directory is the
source of truth. The separate `dbcheck-fiyo` application is outside this scope.

## Prepare a release

Use this layout beneath `/opt/tools/db-check`:

```text
releases/<timestamp>-<short-revision>/   source, bin/db-web, web/.next
current -> releases/<active-release>/
shared/data/                           platform.db, releases/, tasks/
shared/                                private configuration and Python environment
shared/backups/<timestamp>/            prior configuration and complete data snapshot
shared/uploads/                        transferred deployment packages
```

Upload the package to a new staging directory and run `sha256sum -c SHA256SUMS`
before extraction. Create a new release directory, extract `source.tar.gz`,
install `db-web` at `bin/db-web`, and retain revision/checksum metadata there.
Use a unique release ID; an existing directory indicates a prior attempt and
must be inspected before reuse.

The initial deployment reused `/root/lmj/projects/db-check/.venv/bin/python3`.
Its Python 3.13.7 had jsonschema 4.26, python-docx 1.2, and lxml 6.1, and its
requirements matched the deployed revision. Keep that old checkout while this
environment is in use. On updates, verify the interpreter, imports, and changed
requirements before reusing it. If dependencies must change, create a new
versioned environment with `uv venv` under `shared/` and install
`requirements.txt` with `uv pip install --python <venv>/bin/python`.
On this host uv 0.12.23 was bootstrapped with `python3 -m pip install --user uv`.
Point `DBCHECK_PYTHON_BIN` at that environment; the API runs Python report
generation from the new release's source tree. The host initially had stale
HTTP(S)/ALL proxy variables. If downloads fail through that proxy, clear both
uppercase and lowercase proxy variables only for the dependency-install command
with `env -u`; preserve the host's global configuration.

Stage the new configuration as a mode-600 file in the new release directory.
Keep the active `shared/.env` unchanged during preparation. Use these values:

```dotenv
DBCHECK_ADDR=0.0.0.0:18080
DBCHECK_DATA_DIR=/opt/tools/db-check/shared/data
ALLOWED_ORIGINS=http://10.250.0.222:3000
DBCHECK_PYTHON_BIN=<absolute-path-to-validated-python>
DBCHECK_PUBLISH_TOKEN=
PORT=3000
NEXT_PUBLIC_API_MODE=real
NEXT_PUBLIC_API_BASE=http://10.250.0.222:18080
NEXT_PUBLIC_API_PORT=18080
```

Preserve an existing publishing token if one is already configured. When a
fresh deployment needs its first collector release, generate a machine credential
with `openssl rand -hex 32` directly into protected configuration and pass it
to the publisher through a protected environment channel. Keep it out of command
output and packages. PM2 loads the root `.env`; `make web-build`
does not. Build the console with the public API values explicitly exported:

```bash
cd <new-release>/web
npm ci
NEXT_PUBLIC_API_MODE=real \
NEXT_PUBLIC_API_BASE=http://10.250.0.222:18080 \
NEXT_PUBLIC_API_PORT=18080 \
NODE_OPTIONS=--max-old-space-size=2048 npm run build -- --webpack
```

Preparation is complete only when the API binary, Python imports, console build,
configuration, checksums, and available disk space pass validation.

## Backup and cutover

1. Create a private timestamped backup directory. Capture `pm2 jlist` and the
   prior two applications' exact start configuration and environment. PM2 dumps
   can contain secrets; store them with mode 600. Preserve the current symlink
   target, snapshot `shared/.env` if present, record its absence otherwise, and
   record other PM2 applications' state for comparison.
2. Stop only `dbcheck-api` and `dbcheck-web`. Confirm the API process stopped
   before copying its complete data directory, including SQLite WAL/SHM files
   if present. Keep the snapshot under `shared/backups/<timestamp>/`. Confirm
   the copy completed and its files match the stopped source before marking
   it complete. A live filesystem copy is not a consistent SQLite backup.
3. For the initial migration, copy the stopped old data to `shared/data/`.
   For later upgrades, leave `shared/data/` in place after its stopped snapshot.
   Never overlay an older snapshot onto newer data during ordinary deployment.
4. Activate the staged configuration by an atomic rename to `shared/.env`,
   keep mode 600, and link the new release's `.env` to it. Replace only the two
   old PM2 definitions. Start the new release's
   `ecosystem.config.cjs --only dbcheck-api,dbcheck-web --env production`.
   Use the absolute release path so PM2's process definitions identify the
   installed revision. Switch `current` with an atomic symlink rename.
5. Verify below, then run `pm2 save`. Inspect boot startup configuration and
   enable the root PM2 startup service when absent. Compare unrelated process
   definitions/state with the recorded snapshot.

Recovery depends on whether the new version has started writing platform data.

- Before any new-version write, including a failed or incomplete backup, keep
  the original data directory intact. Restore the prior configuration if it
  changed, then restart the prior two PM2 definitions. An incomplete snapshot
  is not a restore source.
- After the new API becomes publicly reachable, preserve its current data and
  stop the failed deployment. Report the failing check and both data locations
  for a recovery decision. Verification can overlap user registration, password
  changes, and report submissions, so automatically restoring the older snapshot
  would discard accepted work. Any snapshot restore now requires the user's
  explicit recovery choice. Reusing current data with old code also requires
  confirmed schema compatibility before restart.
- If a reviewed maintenance access restriction kept the API closed to users
  throughout startup and verification, a migration-only failure can restore a
  verified complete stopped snapshot to an empty data directory. Preserve the
  failed data first, restore prior `shared/.env` or its recorded absence, then
  restore the previous symlink and exact two prior PM2 definitions. Treat API
  startup as a possible write because it can migrate SQLite. The normal public
  cutover above has no such access restriction and takes the preceding path.

Rerun the service checks after either recovery path. Restore only these
applications; `pm2 resurrect` of a full dump can change unrelated applications.
If no complete snapshot exists after a new-version write, preserve all copies
and report the recovery limitation rather than replacing data. Stop after one
failed rollback and report the concrete failing check and preserved paths.
Do not add a host-wide firewall rule as an improvised maintenance mechanism.

## Verify and initialize

Use bounded HTTP checks with timeouts and a short startup retry window:

- Both PM2 applications stay online, use production mode, and point to the new
  release. The API uses `shared/data/` and the selected Python environment.
- `GET /api/auth/me` without a session returns 401 from port 18080.
- The console on port 3000 returns 200; its browser API calls use port 18080.
- A CORS request from `http://10.250.0.222:3000` receives that allowed origin.
- Collector acceptance passes the authenticated listing and package download
  checks in the next section. HTTP availability alone does not complete deployment.
- With an existing active account, package the tracked files
  `tests/fixtures/oracle_os_unprovided/{manifest.json,result.json,collector.log}`
  into a ZIP with those three files at its root. Sign in through
  `/api/auth/sign-in`, submit it as multipart `zips` to
  `/api/reports/generate`, and poll `/api/reports/status/<task_id>` with the
  returned bearer token for at most 300 seconds. Download the returned
  `download_url` and confirm the ZIP contains a readable DOCX report.
  Pass credentials through protected environment/session channels.

`scripts/pm2/smoke_test.sh` requires generated, gitignored MySQL inputs under
`tests/e2e/runs/`; a tracked-source deployment package does not contain them.
Use the tracked Oracle fixture above on this host rather than that script.

If there is no admin, run `bin/db-web admin create --username admin --data-dir
/opt/tools/db-check/shared/data` once. Redirect its output to a private remote
file with `umask 077`; report the file path for credential retrieval. The first
login must retain the required password change. Do not change the admin password
merely to run QA. Without an existing active account, validate report generation
through the repository's diagnostic CLI in an isolated temporary directory.
The tracked `tests/reporter/test_oracle_os_unprovided_orchestrator.py` copies
the same fixture to a temporary directory and verifies the generated DOCX and
metadata. Run it from the release root with the selected Python interpreter:

```bash
uv run --no-project --python <validated-python> python -m unittest discover \
  -s tests/reporter -p test_oracle_os_unprovided_orchestrator.py
```

Explicitly report that authenticated HTTP report verification remains for
the first user login. Preserve the real application's admin and account state.

## Initialize and verify collector downloads

Preserve `shared/data/platform.db` and `shared/data/releases/` together on every
update; deployment source archives contain neither published release metadata
nor release packages. Check the active release catalogue before publishing.
Run the read-only package gate on the target (Python 3.11 or newer):

```bash
uv run --no-project --python <validated-python> python \
  <skill-directory>/scripts/verify_collector.py /opt/tools/db-check/shared/data
```

The gate fails on an empty catalogue, missing platforms, missing or corrupt ZIPs,
and checksum mismatches. It complements the authenticated HTTP checks below.

If no engineer-downloadable recommended release exists, read
`docs/adr/0002-collector-releases-published-by-ci.md`
and the publishing section of `docs/deployment.md`. Use the intranet CI runner
or, until that CI phase lands, its official `scripts/publish_release.sh` workflow:

1. Select a clean Git checkout whose HEAD is exactly on the intended release tag.
   Verify `vX.Y.Z` matches the collector's built-in `Version` in
   `collector/internal/cli/config.go`, with release notes from an annotated tag
   or the documented changelog fallback. An extracted deployment archive lacks
   the Git history this check needs. Use the intended existing tag, or initialize
   the first tag on the selected release commit as documented in
   `docs/deployment.md` when the task authorizes initial publication. Check remote
   tags first and preserve existing tags. If the intended version or commit is
   unresolved, report that specific missing release decision.
2. Configure the same protected `DBCHECK_PUBLISH_TOKEN` on the API and publisher.
   Activate a changed API configuration with the scoped PM2 restart from this
   runbook. Run `scripts/publish_release.sh --url http://10.250.0.222:18080` from
   the tagged checkout. It builds and publishes all four release packages with
   checksums and tag/commit metadata. Preserve the script's failure on conflicts.
3. Using an existing active engineer account, `GET /api/releases` must return a
   `latest` release with Linux/Windows amd64/arm64 packages. A pre-release visible
   only to admins does not satisfy this check. With its bearer token, download
   `GET /api/releases/<version>/packages/<platform>` for each listed platform;
   require HTTP 200, a ZIP attachment, a readable archive containing the collector,
   and SHA256 matching the catalogue. Use an admin's `GET /api/downloads` to
   confirm the engineer's download records. Keep credentials and auth headers out
   of captured output. Confirm the console's Collectors page offers those downloads.

Keep the initial admin's required password change and existing accounts intact.
If an active engineer account is unavailable, run the role-sensitive listing and
download checks with isolated test accounts/data, and report production
authenticated acceptance as pending. Empty catalogues, missing matching tags,
failed publication, and pending production download acceptance are incomplete
deployment results; report the exact remaining step rather than declaring success.

## Initial deployment record

On 2026-10-08, revision `be31ad0` deployed to
`/opt/tools/db-check/releases/20261008-be31ad0`, and `current` points there.
Both production PM2 applications are online. `pm2 save` completed and the
`pm2-root` systemd service is enabled. The unrelated `dbcheck-fiyo` PID remained
unchanged.

`shared/.env` has mode 600. Platform data is under `shared/data/`, including
`platform.db` and the active initial admin. The private initial-admin credential
file is `/opt/tools/db-check/shared/initial-admin.txt`, mode 600. The first
sign-in still requires a password change. `shared/backups/20261008-be31ad0/` contains
`pm2-before.json`, `previous-apps.config.json`, the stopped old `data/` snapshot,
and `pm2-after.json`.

External console HTTP 200, unauthenticated API HTTP 401, and the allowed CORS
origin passed. The isolated Oracle report CLI fixture passed. An isolated API
using the same release and Python environment validated Oracle AWR and two
GaussDB WDR files, accepted a mixed report upload, and downloaded the report ZIP.
DOCX inspection confirmed the AWR SQL ID and metrics and both WDR source rows.
The temporary API and data were cleaned up; production retains only its initial
admin and no QA report tasks, with the required password-change flag still set.

Smoke script, inputs, and downloaded reports are retained at
`/opt/tools/db-check/shared/smoke-20261008-be31ad0`; the verification log is
`/opt/tools/db-check/shared/deployment-smoke-20261008-be31ad0.log`.
Authenticated report upload through the production account remains for the
user's first login.

This historical deployment did not initialize collector releases. Its service
and report checks therefore did not establish complete application acceptance.
Apply the collector checks above to the active deployment.

### Collector initialization correction (2026-10-08)

Published `v1.2.0` from `1f3c4190483123f83b09435f0863af58524db766` through
the official script, with all four platforms marked `latest`. The tag is retained
in the origin repository. The private configuration and SQLite backup are under
`shared/backups/collector-initialization-20261008/`.

`verify_collector.py` failed before publication and passed afterward. An isolated
API using the deployed binary and production release rows/files verified an
engineer's catalogue, four downloads, SHA256, ZIP integrity, unauthenticated
rejection, and four download records. Production accounts were not used; actual
production-session/browser acceptance remains with the user. Verification scripts
are retained under `shared/verify_collector.py` and `shared/verify-downloads.py`.

The host could not reach `proxy.golang.org`; the build used a transient
`GOPROXY=https://goproxy.cn,direct` without changing global configuration. For this
collector revision, the version smoke uses `--version --local --os-only` because
the parser validates connection flags before handling standalone `--version`.

### Console update (2026-10-08)

Deployed `e01b610` to `/opt/tools/db-check/releases/20261008-e01b610`; `current`
points there. Only `web/src` and skill files changed since `1f3c419`, so the API
code, schema, Python requirements, and PM2 configuration were unchanged; the
Python environment was reused and no migration ran. The package was built on a
developer mac with `scripts/package.sh` and uploaded to
`shared/uploads/20261008-e01b610/`, which also holds `prepare-e01b610.sh`,
`cutover-e01b610.sh` (the 1f3c419 update script with revisions substituted), and
`cutover.log`. The staged `.env` was a copy of the active one, preserving the
publishing token.

The backup is `shared/backups/20261008-e01b610/`, with a stopped `data/` snapshot,
`previous.env`, `previous-release`, `previous-apps.config.json`, and PM2 state
before and after. External console HTTP 200, unauthenticated API HTTP 401, the
allowed CORS origin, both production processes on the new release, and an
unchanged `dbcheck-fiyo` PID passed; `pm2 save` completed. `verify_collector.py`
passed for `v1.2.0` on all four platforms. Authenticated engineer listing and
download through a production account remain with the user.

A rexec-synced workspace omits `.env*` files, so `package.sh` reports tracked
changes for `web/.env.example`; restore it with `git checkout` before packaging.
