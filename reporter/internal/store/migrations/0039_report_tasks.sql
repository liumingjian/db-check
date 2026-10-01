-- Report tasks (CONTEXT.md): a submitted batch of report items, owned by its
-- submitter. Records are kept forever; their files under tasks/<id>/ expire.
-- status: queued (accepted, waiting for the worker), processing, done, failed.
-- The console's contract shows queued as processing.
CREATE TABLE report_tasks (
    id           TEXT PRIMARY KEY,
    submitter_id TEXT NOT NULL REFERENCES users (id),
    status       TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'done', 'failed')),
    error        TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL
);

CREATE INDEX report_tasks_submitter ON report_tasks (submitter_id, created_at);
CREATE INDEX report_tasks_status ON report_tasks (status, created_at);

-- Report items, in submission order (position is 1-based and names the
-- item's upload files and its items/<position>/ directory). db_type and
-- collector_version are what the server read from the uploaded ZIP;
-- collector_version is NULL when unknown. A failed item keeps its reason.
CREATE TABLE report_items (
    task_id           TEXT NOT NULL REFERENCES report_tasks (id) ON DELETE CASCADE,
    position          INTEGER NOT NULL,
    file_name         TEXT NOT NULL,
    db_type           TEXT NOT NULL CHECK (db_type IN ('mysql', 'oracle', 'gaussdb')),
    collector_version TEXT,
    status            TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'done', 'failed')),
    reason            TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (task_id, position)
);

CREATE INDEX report_items_collector_version ON report_items (collector_version);
