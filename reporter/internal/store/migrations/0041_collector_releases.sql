-- Collector releases (CONTEXT.md, ADR 0002). Only the publish API creates
-- them; admins change status. The version is the key: download records
-- (#43) and collector notices (#44) refer to it.
CREATE TABLE releases (
    version       TEXT PRIMARY KEY,
    tag           TEXT NOT NULL UNIQUE,
    commit_sha    TEXT NOT NULL,
    published_at  TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('pre-release', 'latest', 'deprecated', 'revoked')),
    revoke_reason TEXT,
    notes         TEXT NOT NULL DEFAULT '',
    db_types      TEXT NOT NULL, -- JSON array, e.g. ["mysql","oracle"]
    CHECK ((status = 'revoked') = (revoke_reason IS NOT NULL))
);

-- At most one latest release; none is valid.
CREATE UNIQUE INDEX releases_one_latest ON releases (status) WHERE status = 'latest';

-- Release packages: exactly the four platforms per release. The file lives
-- at <data dir>/releases/<version>/<file_name>.
CREATE TABLE release_packages (
    version   TEXT NOT NULL REFERENCES releases (version) ON DELETE CASCADE,
    platform  TEXT NOT NULL CHECK (platform IN ('linux-amd64', 'linux-arm64', 'windows-amd64', 'windows-arm64')),
    file_name TEXT NOT NULL,
    size      INTEGER NOT NULL CHECK (size > 0),
    sha256    TEXT NOT NULL,
    PRIMARY KEY (version, platform)
);
