# Deploying the platform (db-web + web console)

The platform is two processes on one host, managed by PM2:

- `dbcheck-api`: the `db-web` backend (`reporter/cmd/db-web`). It holds accounts, sessions, collector releases, download records, and report tasks, and it runs report generation.
- `dbcheck-web`: the Next.js console in `web/`.

Deploy both together. The console talks to the real backend by default, and it only works against a `db-web` of the same version.

## Requirements

- Go 1.24+, Python 3.10+ with `requirements.txt` installed (a `.venv` at the repository root is picked up automatically), Node.js 20+.
- PM2 (`npm install -g pm2`).
- On the host that publishes collector releases: `git` and `zip`.

## The data directory is the complete state

`DBCHECK_DATA_DIR` holds everything the platform knows, and nothing lives anywhere else:

| Path | Contents | Lifetime |
|---|---|---|
| `platform.db` | SQLite: users, account actions, sessions, collector releases, download records, report task records (ADR 0003) | forever |
| `releases/<version>/` | Release packages `db-collector-<version>-<platform>.zip` | forever |
| `tasks/<id>/` | Uploaded ZIPs, extracted data, generated reports of one report task | deleted 30 days after submission; the record in `platform.db` stays (the task shows as 已过期, expired) |

Use a persistent directory such as `/var/lib/dbcheck`. Never use `/tmp`. The PM2 default `/tmp/dbcheck-data` is for local development only.

The schema is created and upgraded automatically when `db-web` starts. An upgrade needs no manual migration step.

### Backup

Take an online backup of the database while `db-web` runs:

```bash
sqlite3 "$DBCHECK_DATA_DIR/platform.db" ".backup '/backup/dbcheck/platform-$(date +%F).db'"
```

Copy `releases/` as well (it never changes once written), and `tasks/` if the report files of the last 30 days matter. Restoring means putting `platform.db` and those directories back into an empty data directory before starting `db-web`. Scheduling backups is up to the operator.

## Configuration

Copy `.env.example` to `.env` at the repository root. `ecosystem.config.cjs` loads it and never overrides variables already exported. For a console run outside PM2, `web/.env.example` lists the console's variables.

### Backend (`dbcheck-api`)

| Variable | Meaning |
|---|---|
| `DBCHECK_ADDR` | Listen address. Default `127.0.0.1:8080`. |
| `DBCHECK_DATA_DIR` | Required. The data directory above. |
| `ALLOWED_ORIGINS` | Required. Console origins allowed by CORS, comma-separated: full origins (`http://10.0.0.5:3000`, recommended), `host:port`, or `*` (local testing only). A `localhost` or `127.0.0.1` entry with a port also admits the same port on the host's LAN addresses. |
| `DBCHECK_PUBLISH_TOKEN` | The CI machine credential for `POST /api/ci/releases`. Unset or empty disables the publish API. Generate one with `openssl rand -hex 32`. |
| `DBCHECK_PYTHON_BIN` | Python for report generation. Default: `.venv/bin/python3` when present, else `python3`. |

`DBCHECK_API_TOKEN` (the old shared token) is retired. `db-web` ignores it and logs a warning at startup while it is still set; remove it.

`db-web` flags: `--addr`, `--data-dir`, `--allowed-origins`, `--max-upload-bytes` (default 1 GiB, 0 disables), `--log-replay-lines`, `--python-bin`, and `--retention-ttl` (only for report tasks left from before the SQLite cutover, which still expire 24 hours after they finish).

### Console (`dbcheck-web`)

Next.js inlines these at `npm run build`. Set them before building; changing them later needs a rebuild.

| Variable | Meaning |
|---|---|
| `NEXT_PUBLIC_API_MODE` | `real` when unset: the console talks to `db-web`. `mock` keeps demo data in the browser's local storage and needs no backend; use it only on purpose (`npm run dev:mock`). There is no fallback between the two. |
| `NEXT_PUBLIC_API_BASE` | The full origin of `db-web` as the browser reaches it, for example `http://10.0.0.5:8080`. |
| `NEXT_PUBLIC_API_PORT` | Used when `NEXT_PUBLIC_API_BASE` is unset: a page served on `:3000` calls the same host on this port. PM2 derives it from `DBCHECK_ADDR`. Otherwise the page's own origin is used. |

## Start with PM2

Install the dependencies once:

```bash
python3 -m venv .venv && source .venv/bin/activate
make init-python
make web-install
```

Production (compiled backend, built console):

```bash
make build-db-web
make web-build        # with the console variables above exported or in .env
make pm2-start-prod
```

Development (`go run` and `next dev`): `make pm2-start`. After changing `.env`, run `make pm2-restart` to reload the environment. `make pm2-status` and `make pm2-logs` show the state.

Remote Linux host example (`.env`):

```bash
DBCHECK_ADDR=0.0.0.0:18080
DBCHECK_DATA_DIR=/var/lib/dbcheck
ALLOWED_ORIGINS=http://10.250.0.222:3000
DBCHECK_PUBLISH_TOKEN=<openssl rand -hex 32>
```

Opening `http://10.250.0.222:3000` then calls `http://10.250.0.222:18080`.

## First deploy

A fresh data directory has no users and no collector releases. Before anyone can use the console:

1. **Create the first admin.** On the server, with the same data directory `db-web` uses:

   ```bash
   DBCHECK_DATA_DIR=/var/lib/dbcheck bin/db-web admin create --username admin
   ```

   It prints a temporary password once. The first sign-in must change it. The command refuses when any admin exists, so it cannot be used again later. Further accounts register in the console and an admin approves them.

2. **Set `DBCHECK_PUBLISH_TOKEN`** in `.env` (or the environment), then `make pm2-restart`. Give the same value to whoever publishes releases.

3. **Publish the current collector release** with the publish script (next section). The first publish needs an annotated `v1.2.0` tag on the current release commit. The repository's older tags are named `release-X.Y.Z`; the script accepts only `vX.Y.Z`.

   ```bash
   git tag -a v1.2.0 <current release commit> -m "- <release note>"
   ```

   Until a release is published, 采集器 (Collectors) shows «暂无推荐版本，请联系管理员» (no recommended version).

Then sign in as the admin, change the temporary password, and check 采集器 lists `1.2.0` as latest.

## Publishing a collector release

Collector releases come only from a git tag (ADR 0002). Until the CI phase, a person runs `scripts/publish_release.sh` on a release host; CI will call the same script.

Preconditions:

- The server has `DBCHECK_PUBLISH_TOKEN` set, and the release host has the same value.
- The release host has Go, `git`, and `zip`, and runs the script from the repository root of a clean checkout (no uncommitted changes to tracked files).
- HEAD is exactly on a `vX.Y.Z` or `vX.Y.Z-rcN` tag, and the tag without its `v` equals the collector's built-in version (`Version` in `collector/internal/cli/config.go`, printed by `db-collector --version`). For a pre-release, set `Version` to `X.Y.Z-rcN` before tagging.
- Release notes come from the tag annotation (`git tag -a`). A lightweight tag falls back to the `## [X.Y.Z]` section of `CHANGELOG.md` (the file is optional). With neither, the script refuses.

```bash
git checkout v1.2.0
export DBCHECK_PUBLISH_TOKEN='<the server value>'
scripts/publish_release.sh --url https://dbcheck.example.com
```

The script:

1. checks the tag against the built-in version, the clean checkout, and the release notes, and stops before building if any check fails;
2. builds the four release packages with `scripts/build_release_packages.sh`: `dist/db-collector-<version>-{linux,windows}-{amd64,arm64}.zip` (`--dist-dir` changes the directory);
3. computes their SHA256 and posts them with the version, tag, commit, release notes, and supported database types (maintained in the script: mysql, oracle, gaussdb; see `reporter/internal/publish`) to `POST /api/ci/releases`.

A `vX.Y.Z` tag publishes as latest, and the previous latest becomes deprecated. A `vX.Y.Z-rcN` tag publishes as a pre-release that only admins see. The packages are reproducible, so rerunning the same tag is safe: identical content answers `already published ... nothing changed`. The same version with different content (for example a moved tag) answers 409 and the script fails.

> Re-publishing only matches byte for byte when the rebuild uses the same toolchain (the same Go version and `zip`) as the first publish. A 409 on a rerun means the rebuilt packages differ from the stored ones: check the toolchain before suspecting the tag.

## Smoke test

With an active account that has already changed its temporary password:

```bash
DBCHECK_SMOKE_USERNAME=<username> DBCHECK_SMOKE_PASSWORD=<password> make pm2-smoke
```

It uploads the bundled MySQL e2e ZIP, polls the task, and downloads the result.

The HTTP API is documented in `docs/openapi/dbcheck-web.yaml`.
