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

Preserve an existing publishing token if one is already configured. A fresh
deployment leaves publishing disabled. PM2 loads the root `.env`; `make web-build`
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

Collector release upload is separate. A fresh deployment may correctly show no
recommended collector until an authorized collector publication occurs.

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
