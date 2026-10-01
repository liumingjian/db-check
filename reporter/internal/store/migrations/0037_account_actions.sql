-- Account actions: the append-only history of admin actions on a user,
-- each with the acting admin and time. Rows are only ever inserted.
CREATE TABLE account_actions (
    seq     INTEGER PRIMARY KEY AUTOINCREMENT, -- append order: oldest first
    user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    action  TEXT NOT NULL CHECK (action IN ('approve', 'reject', 'disable', 'enable', 'promote', 'demote', 'reset')),
    by_id   TEXT NOT NULL REFERENCES users (id),
    at      TEXT NOT NULL
);

CREATE INDEX account_actions_user_id ON account_actions (user_id);
