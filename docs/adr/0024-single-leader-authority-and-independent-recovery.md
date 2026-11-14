# ADR 0024: Single-leader authority and independent recovery

Status: accepted for the C06-01 scenario. Date: 14 November 2026.

## Decision

Use PostgreSQL physical replication and the actual Atlas Inventory feature for durability experiments. Keep received/flushed WAL separate from replay visibility and distinguish successful acknowledgments from uncertain request outcomes. Use a small independent Go model for finite schedules, retention gaps and failure-domain assumptions.

Require a confirmed old-primary process fence before local promotion. After promotion, advance an opt-in Inventory writer epoch and reject stale application incarnations at the resource. Hold the authority row's shared lock through commit, so an epoch change waits for admitted transactions. Restrict the application role's privileges and fix the security-definer trigger search path.

Use `pg_rewind` before an old primary rejoins. Never merge its divergent branch implicitly. Use a verified physical backup plus immutable archived WAL to restore an independent target before a replicated logical operator error. Validate business invariants through SQL and Atlas HTTP.

## Consequences and limits

The one-host process controller supplies a testable physical fence, without claiming to solve distributed election or partition fencing. A local epoch copied into two databases cannot supply that guarantee. The optional Inventory guard requires coordinated configuration of API/workers; other contexts and production role hardening remain open.

Synchronous remote flush can acknowledge a write while a read from the standby is stale. `remote_apply` strengthens visibility for that standby, while depending on its availability. Client cancellation during a synchronous wait can leave a committed transaction whose outcome must be reconciled by intent identity.

Replica availability and backup recoverability require different evidence. The point-in-time restore deliberately omits one post-target acknowledged write and reports it. Local tmpfs recovery measurements and modeled placement correlations do not establish cloud availability, independent disks, production RPO/RTO, backup retention or disaster recovery policy.
