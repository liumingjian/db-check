# Web task lifecycle design

Behavior below was accepted in discussion decisions Q1 through Q13. Sections marked proposed await final design review. This document does not authorize implementation. Domain terms are defined in [CONTEXT.md](../../CONTEXT.md); the storage trade-off is recorded in [ADR 0001](../adr/0001-web-task-lifecycle-ownership.md).

## Problem and evidence

- `reporter/internal/web/http_handler.go:335` drops work when the channel is full, although the handler has already created a task and later returns success.
- `http_handler.go:144` creates the task before the remaining upload validation and persistence finish. Later request failures can leave a task that never enters execution.
- `http_handler.go:344` restores tasks before starting the worker. `resume.go:12` sends every unfinished task into the same channel, so a backlog above 32 can lose scheduling requests.
- `http_handler.go:411` and subsequent state updates ignore persistence errors before emitting progress or completion.
- `task_store.go:126` uses temporary-file rename for one JSON file, without file or directory synchronization. It does not publish uploads and metadata together.
- `retention.go:10` removes expired terminal tasks only. `config.go:22` defaults terminal retention to 24 hours; zero disables cleanup.

These are source observations. No load reproduction or implementation tests have been run for this design.

## Accepted behavior

### Admission and execution

- Capacity counts accepted unfinished tasks, including queued and executing tasks. Default capacity is 33 and is configurable. Memory-channel capacity is only a scheduling detail.
- Keep one worker and execute tasks in acceptance-time order, including restart recovery. No priorities or interleaving between tasks are added.
- A recovered backlog above the configured limit remains valid. Admit new work only after the unfinished count falls below the limit.
- Confirm acceptance only after input validation, input persistence, and admission succeed. Rejected submissions clean up temporary files and leave no apparent queued or processing task.
- At capacity, immediately return HTTP 503 with busy/retry-later feedback. Retain the browser's selected files for manual retry; do not wait for a slot, re-upload automatically, or promise a wait estimate.
- Keep one API process and file storage. Complete inputs in a staging directory on the same filesystem before publishing the task. Use a persistent data directory rather than relying on `/tmp/dbcheck-data`.
- Recover accepted work after process crashes. Multi-instance shared execution, power-loss durability, and media-failure recovery are outside this design.

### Task outcomes and retries

| State | Meaning |
| --- | --- |
| `queued` | Durably accepted and waiting for execution. |
| `processing` | Executing the task, including final result assembly. |
| `done` | Processing ended and a result ZIP is available; some items may have failed. |
| `failed` | All items failed, final ZIP generation failed, or a task-level failure prevents completion. |

Submission returns the actual current state. Frontend and OpenAPI represent both queued and executing tasks. Status exposes success and failure counts and failed-item details; the UI explicitly labels mixed outcomes as partial success rather than displaying an unqualified success message.

After restart, preserve completed item results and failed item outcomes. Rerun interrupted or unfinished items from their beginning, allowing derived files to be regenerated. Do not add step-level checkpoints or automatically retry failed items. Implementation must verify external side effects are safe when an interrupted item runs again.

Manual retry creates a new task from resubmitted selected files with a new idempotency key. The original outcomes remain unchanged; no in-place retry endpoint is added.

### Persistence faults and client reconciliation

When state persistence fails, pause new admissions and starting further items. A running item may finish, but its state progress and completion notifications must wait for successful persistence. Surface the storage fault, preserve the last saved state, and do not pretend a new failed state was saved. Operators fix storage and restart the process to recover; no automatic live recovery loop is added.

The browser retains task IDs and retrieves server status, item results, and download information after refresh or reconnect. WebSocket provides live updates; use low-frequency status queries while disconnected. Full historical log replay is excluded, and the UI indicates logs may be incomplete.

### Submission idempotency and retention

- The same key and same inputs return one original task, including concurrent requests and retries after a lost successful response.
- Bind the key to file contents, original names, input pairing, and order. A retained key with different inputs returns HTTP 409 without changing the original task.
- An explicitly new submission uses a new key. Submission deduplication does not retry failed items.
- The guarantee lasts while the task is retained. Keep key information for the same lifetime as its task. After task cleanup, reuse of an old key may create a new task.
- Preserve configurable terminal retention, currently 24 hours by default. Unfinished tasks remain retained; disabling cleanup also retains their terminal task and key records indefinitely.

## Proposed module interface

`TaskLifecycle` owns admission, input staging, publication, dispatch, state changes, recovery, notification ordering, and coordinated retention. HTTP handlers perform authentication and transport parsing, then call the module. WebSocket handlers consume its watch interface instead of assembling lifecycle rules themselves.

| Entry point | Contract |
| --- | --- |
| `Submit(ctx, submission)` | Accept ordered input streams and optional submission key; return a task snapshot or a typed rejection. Own staged-file cleanup and key resolution. |
| `Get(ctx, taskID)` | Return the last persisted task snapshot, item outcomes, and available download information. |
| `Watch(ctx, taskID)` | Establish a coherent initial snapshot and subsequent updates, or require reconciliation after interruption. |
| `Start(ctx)` / `Close(ctx)` | Acquire/release writer ownership and manage recovery, dispatch, retention, and shutdown. |

These are conceptual signatures, not a new public Go API commitment. Preserve existing Pipeline behavior and reuse TaskStore and the log hub inside the module. Move lifecycle orchestration out of handler methods rather than wrapping the existing handler with a forwarding layer. Add internal test seams only where fault injection or actual alternative behavior requires them.

## Proposed submission and publication protocol

1. Complete startup recovery before accepting submissions. Acquire exclusive ownership of the data directory; reject startup if another writer owns it. The locking mechanism must match supported operating systems and release ownership after a crash.
2. Serialize key resolution, admission reservations, publication bookkeeping, and cleanup decisions inside the single process. Count unfinished tasks plus new-submission reservations when admitting another request, preventing concurrent overcommit. Reservations are not accepted tasks and are not exposed as queued.
3. For a retained key, verify the payload identity and return its task even when new-task capacity is full, provided storage is healthy. Concurrent submissions for a new identical key coordinate one publication. Bound outstanding upload work and release reservations on rejection.
4. Write new inputs into a private staging directory excluded from task discovery. Validate the complete submission and calculate a digest of its ordered semantic inputs; multipart boundaries do not affect identity. Store the key and digest in task metadata, not a separately committed index.
5. Under the publication gate, assign acceptance ordering and write complete queued metadata alongside the inputs. Rename the complete staging directory into a fresh final task directory on the same filesystem. Never overwrite an existing task directory. That rename is the acceptance boundary.
6. Before publication, cancellation or validation failure removes staged input and releases the reservation. After publication, response loss or request cancellation must not delete the accepted task. Reconcile uncertain publication outcomes from the final directory; do not blindly recreate or delete them.
7. Update the in-memory task/key indexes, convert the reservation to accepted occupancy, and wake the dispatcher. Accepted task records remain authoritative if the process crashes before notification or dispatch. Return the current persisted task state.

Use acceptance timestamps with a stable persisted tie-breaker for FIFO order. Upload start time does not reserve a queue position. A failed wake notification cannot discard a task: the dispatcher must drain and recheck the authoritative pending set before sleeping, with startup reconstruction from disk.

## Proposed execution, recovery, and notifications

- Startup scans published tasks, reconstructs capacity and key indexes, and removes abandoned staging data before readiness. Existing records without key metadata remain recoverable but receive no retroactive deduplication guarantee.
- Treat unreadable or corrupt task records as a startup fault requiring operator attention, rather than silently freeing unknown occupancy. If a readable task has missing or invalid required inputs, persist an explicit task failure when storage permits. A save failure instead enters the global storage-fault pause.
- Restore interrupted tasks to scheduling in acceptance order. Preserve terminal item outcomes. Persist the transition to processing before executing the next item. Save each item outcome before reporting durable progress or starting the next item.
- Verify retained completed-item report artifacts before assembling the ZIP. Missing or unusable retained artifacts fail the task explicitly; do not silently omit a supposedly successful item or automatically rerun it.
- Build the ZIP under a temporary name and publish it only after successful close and validation. Persist done only after a valid final ZIP exists. A crash between ZIP publication and saving done recovers by rebuilding the final artifact from saved item outcomes.
- Persistence failure pauses admissions, subsequent execution, and retention mutations. Existing readable snapshots remain available with an explicit storage-fault indication. Operator repair and restart perform reconciliation; no unsaved state is presented as committed.
- Establish watch subscriptions and snapshots coherently so completion cannot fall between them unnoticed. If a bounded subscriber queue loses state updates, disconnect that subscription and require a new snapshot; log history remains best effort.
- Serialize retention with key lookup and publication. Remove a terminal task and its key mapping together from the module's view; reconstruct mappings only from surviving published tasks after restart. Interrupted or failed cleanup must not leave a task eligible for execution as if it were newly accepted.
- On shutdown, stop admission and starting new work, finish or cancel current work within the existing execution cancellation contract, and leave unfinished persisted work for restart recovery.

## Proposed compatibility policy

- Preserve HTTP 200 on successful submission, extending the response status enum instead of changing success to HTTP 202.
- The browser always sends a key, retains it while a submission outcome is uncertain, and replaces it when the user explicitly starts a new submission or changes the input set. Retaining keys does not make browser file selections survive a full reload; re-upload can require selecting the same files again.
- Allow legacy API clients to omit the key, creating a new task without deduplication. **This optional-key compatibility rule requires final user confirmation.** Keyed requests use the binding and lifetime rules above.
- Return machine-readable error codes distinguishing capacity exhaustion, unavailable storage, and conflicting key reuse. Keep the storage fault distinct from a persisted task failure.
- Update frontend behavior and OpenAPI together, including mixed outcomes, failed-item details, refresh recovery, and busy feedback. Existing unauthenticated access is not expanded.

## Acceptance scenarios

| Scenario | Required observation |
| --- | --- |
| Capacity full and simultaneous new requests | HTTP 503 for excess requests; no overcommit or phantom tasks. |
| Malformed input, failed upload, pre-publication cancellation | No accepted task; staged input and reservations cleaned up. |
| Concurrent same key and input; same key with different input | Exactly one original task; conflict returns 409 without overwrite. |
| Crash before publication | No partial task is scheduled; abandoned staging is recoverable cleanup. |
| Crash or lost response after publication, before dispatch | Same-key retry resolves original task; restart executes it. |
| More than 32 saved unfinished tasks; configured capacity lowered | All remain recoverable in FIFO order; excess backlog blocks new admission. |
| Crash during an item or after output generation but before its state save | Unfinished item reruns; saved done/failed items retain their outcomes. |
| Mixed item results, all failures, or ZIP failure | Partial-success details with downloadable done result, or appropriate failed outcome. |
| State save fails before execution, after an item, or at terminal transition | Pause further work; no corresponding premature progress or completion event. |
| Refresh, disconnect, completion during subscription setup, subscriber overflow | Authoritative state and download availability recover without requiring log replay. |
| Terminal retention cleanup races with keyed retry | One serialized result; retained task deduplicates, cleaned task may be newly submitted. |
| Legacy task records and optional unkeyed clients | Existing tasks recover; unkeyed submissions have no deduplication promise. |
| Second writer starts on the same directory | Startup fails before admission, recovery execution, or cleanup. |
| Corrupt metadata or missing completed-item artifacts | Explicit startup fault or task failure; no silent loss or misleading done result. |

Exercise these through the lifecycle entry points and HTTP/client integration where relevant. Use real temporary task directories for crash and retention tests, controlled command execution for report generation, and targeted storage fault injection. Tests have not been implemented or run.

## Final review

Confirm the proposed module interface, publication and recovery mechanics, and compatibility policy as the implementation design. The explicit remaining product choice is whether legacy clients may omit an idempotency key. Approval of this design records shared understanding; implementation requires a separate instruction.
