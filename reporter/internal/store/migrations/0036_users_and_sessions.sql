-- Users (CONTEXT.md): one role and one account status each.
-- role is the wire value: 'admin' or 'user' (the engineer role).
CREATE TABLE users (
    id                   TEXT PRIMARY KEY,
    username             TEXT NOT NULL UNIQUE,
    display_name         TEXT NOT NULL,
    role                 TEXT NOT NULL CHECK (role IN ('admin', 'user')),
    status               TEXT NOT NULL CHECK (status IN ('pending', 'active', 'rejected', 'disabled')),
    password_hash        TEXT NOT NULL,
    must_change_password INTEGER NOT NULL DEFAULT 0,
    email                TEXT UNIQUE,
    team                 TEXT NOT NULL DEFAULT '',
    note                 TEXT NOT NULL DEFAULT '',
    reason               TEXT,
    applied_at           TEXT NOT NULL
);

-- Sessions: only the SHA-256 of the opaque token is stored.
CREATE TABLE sessions (
    token_hash TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    issued_at  TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE INDEX sessions_user_id ON sessions (user_id);
