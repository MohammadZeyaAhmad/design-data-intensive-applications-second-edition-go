# ADR 0025: Immutable objects and conditional generation publication

Status: accepted for the C06-02 increment. Date: 14 November 2026.

## Decision

Keep authoritative Inventory in PostgreSQL. Model object-backed storage with bounded warehouse read snapshots in the independent Go lab. Upload content-addressed immutable segments, then immutable manifests, then publish through one conditional current-head update. Use create-if-absent for immutable objects and initial head creation; use an opaque version token for later head CAS.

Keep command identity and complete receipt history inside the immutable generation. Reconcile ambiguous publication by that identity. Return an explicit conflict when another command changes the base; do not automatically retry a stale read-modify-write. Identical retries resolve to the original generation, and changed payloads under the same ID fail.

Retain every published manifest and its segment references. Named snapshots are immutable. Cache only verified immutable bodies and fetch the current head on every current read. Compaction changes the current view's references through the same protocol, without deleting historical data.

Use a deterministic memory adapter, an fsynced file adapter with process locks, and an actual AWS SDK S3 adapter. Validate shared contracts over signed Moto HTTP requests and expose an explicit real-bucket target. Disable hidden SDK retries for ambiguous conditional writes.

## Consequences and limits

Atomic visibility spans many immutable objects through one head CAS, conditional on strong per-key visibility and atomic provider preconditions. It is not a general cross-key transaction, distributed election service or an implementation of S3. One hot head and finite manifests/receipts impose explicit throughput/history limits.

Pre-publication crashes leave orphan candidates without exposing rows. Retrying the requested command reuses immutable data. Quiescent inventory cannot authorize deletion during concurrent publication; online GC and snapshot expiration need a separately fenced lifecycle policy. Retained history means compaction alone does not reclaim storage.

Emulator success is contract evidence, without establishing cloud durability, IAM, encryption, regional failure behavior or storage-service crash persistence. The external contract uses a fresh owned prefix in an existing bucket and verifies cleanup after compute stops. Production publisher permissions and retention remain separate work.
