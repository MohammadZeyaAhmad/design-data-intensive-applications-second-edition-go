# ADR 0002: Database deadlines and an independently owned event store

Status: accepted for the second implementation increment.

An inventory hold expires according to the database clock. Commitment checks the clock after its row locks have been acquired; transaction-start time would incorrectly extend a hold while a command waits. Background expiry claims due active reservations with row locks and `SKIP LOCKED`, then releases quantity and appends the fact in the same transaction. Committed allocations have a separate explicit reversal transition. Business workflow ownership will later determine when a dispatched allocation can no longer be reversed.

Ordering uses an independent schema and aggregate. It cannot import Inventory's private domain or persistence code; architecture tests enforce this. Both HTTP adapters use shared tenant authentication and bounded JSON transport, while their errors and business decisions remain context-owned.

An order stream head provides expected-version concurrency. Events, outbox records and saved request identity share a transaction. The event log rejects ordinary update/delete operations. Reads use a repeatable-read snapshot so the source head, optional cache and suffix belong to a consistent database view.

Snapshots store a versioned canonical representation with a checksum and can always be discarded. Legacy facts are upcast when read, without editing their stored schema/payload. A future unknown schema causes explicit failure and consumer quarantine.

Reconstruction and incremental consumers acquire the same projection advisory lock before view/outbox locks. Reconstruction then takes a source-head lock, replays a bounded prefix and atomically replaces that order's view and progress. The lock is scoped to tenant/order; its hash may serialize a rare colliding key, but cannot grant two owners of the same key. This provides a concrete single-stream baseline for a later all-stream generation migration.

Integration suites create/drop their own randomly named database under an explicit local test binding. This lets tests preserve the immutability contract instead of disabling event triggers for cleanup. Cloud deployments and normal application roles do not require database-creation privileges.
