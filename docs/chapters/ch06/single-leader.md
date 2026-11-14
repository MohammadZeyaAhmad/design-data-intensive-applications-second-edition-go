# Single-leader replication in the Atlas project

| Source section | Executable evidence |
| --- | --- |
| B06-001 — Backups and Replication | A real logical stock-table deletion reaches the standby; independent base-backup/WAL recovery restores the pre-error invariants |
| B06-002 — Single-Leader Replication | Actual PostgreSQL primary/standby, standby read-only checks and Atlas Inventory writes to one authority; corresponding bounded Go model |
| B06-003 — Synchronous versus Asynchronous Replication | Separate async, remote-flush and remote-apply acknowledgment/loss/lag measurements; real synchronous waits and uncertain-commit reconciliation |
| B06-004 — Setting Up New Followers | Consistent online physical snapshot, streamed WAL, physical slot and catch-up; model snapshot/suffix and retention-gap behavior |
| B06-006 — Handling Node Outages | Actual follower stop/restart, primary process crash, fail-closed old API and guarded promotion |
| B06-007 — Follower Failure: Catch-up Recovery | Retained WAL catches up a stopped follower; actual rewind reintroduces a divergent old primary read-only |
| B06-008 — Leader Failure: Failover | Confirmed physical process fence before promotion, new Inventory epoch, old-token rejection and divergent branch discard |
| B06-015 — Replication Across Multiple Datacenters | Explicit zone/region common-mode failure model and placement assumptions; the native sandbox is colocated and makes no cross-zone availability claim |

P30 starts with an explicit primary-write/replica-read contract and a WAL replay-position gate. A paused replica demonstrates a stale read before the gate rejects it. Complete session tokens, sticky routing, monotonic reads and consistent-prefix remedies remain C06-04. P41 starts with an actual corruption rejection and independent point-in-time restore, measured local recovery time and explicit loss window. Retention, offsite recovery, access/encryption policy and production recovery objectives remain OPS-03.

The Go model has no distributed election protocol. Its deliberately unsafe independently writable copies demonstrate why locally copied epochs do not establish exclusive authority. Its safe promotion rejects an absent old-primary fence. Physical PostgreSQL WAL, not this model, supplies the native durability evidence.

The [guide](../../REPLICATION.md) connects the [implementation](../../../labs/ch06/single-leader), [writer-fence tests](../../../tests/integration/writer_fence_test.go) and [scenario](../../../scenarios/single-leader-replication.json). Full source-heading/sub-concept, operational and scale audits remain mandatory against the unchanged baseline.
