---
status: accepted
---

# Keep web task coordination in one writer over file storage

The web deployment currently runs one API process, and its report inputs and outputs already live in task directories. Keep this deployment scope and give one lifecycle module ownership of admission, scheduling, state persistence, recovery, and notification ordering while retaining Pipeline execution. Accepted tasks must remain recoverable from stored task data rather than depending on the contents of an in-memory channel.

Adding a database or broker would introduce deployment and migration work beyond this local replacement. File storage instead requires explicit publication and single-writer coordination; a second process must not independently recover the same tasks. Use a persistent data directory. This decision covers process-crash recovery, not multi-instance execution, power-loss durability, or media-failure recovery. Revisit it before supporting shared execution across API processes.

The behavioral contract and proposed mechanics are maintained in the [web task lifecycle design](../design/web-task-lifecycle.md).
