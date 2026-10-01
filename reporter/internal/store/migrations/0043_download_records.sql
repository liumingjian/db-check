-- Download records (CONTEXT.md): which user downloaded which release
-- package, and when. Written when a package download starts; rows are only
-- ever inserted.
CREATE TABLE download_records (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT, -- insert order, breaks ties on `at`
    id       TEXT NOT NULL UNIQUE,
    user_id  TEXT NOT NULL REFERENCES users (id),
    version  TEXT NOT NULL,
    platform TEXT NOT NULL,
    at       TEXT NOT NULL,
    FOREIGN KEY (version, platform) REFERENCES release_packages (version, platform)
);

CREATE INDEX download_records_at ON download_records (at);
