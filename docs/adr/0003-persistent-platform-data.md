---
status: proposed
---

# Keep long-lived platform data in a single SQLite file

Users, account status history, collector releases, download records, and report task records must outlive any single task, and admins edit them concurrently. ADR 0001's file-per-task storage and 24-hour retention cannot hold them. The deployment stays a single API process. That process keeps these records in one SQLite file in the persistent data directory. Uploaded ZIPs, release packages, and generated reports stay as files on disk. Report task records are kept permanently. Their uploaded inputs and generated reports are deleted after 30 days, because the ZIPs contain customer data.

This supersedes ADR 0001 on storage and retention only. ADR 0001's single-writer lifecycle (admission, scheduling, recovery, notification ordering) still holds. Accept this ADR when backend work on users and collector releases begins.

## Considered Options

- **JSON files per record type**: rejected. Concurrent admin edits and queries such as "download records by user" would need a hand-written index and locking.
- **PostgreSQL/MySQL server**: rejected. It is an extra service to deploy and operate for one intranet process. Revisit it together with ADR 0001 before running several API processes.
