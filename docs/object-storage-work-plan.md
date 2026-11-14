# C06-02 implementation increment

Date: 14 November 2026. Preserve the complete baseline in `docs/plan/`.

1. Define a bounded object-store port with opaque version tokens, create-if-absent and compare-and-swap publication. Provide durable local and signed S3 HTTP adapters; run the same conditional-write contract against both.
2. Implement content-addressed immutable warehouse-availability segments, immutable versioned manifests, atomic current-generation publication, intent reconciliation, snapshots and bounded verified reader caches. Keep PostgreSQL Inventory as application authority.
3. Exercise independent compute processes, competing writers, process kills before/after publication, lost replies, snapshot retention, compaction and orphan reuse. Verify complete generations and source invariants; record API calls, bytes, conflicts and cache effects.
4. Provide a pinned S3-compatible service profile plus an explicit opt-in real S3 contract target, without silently skipping a selected integration. Record consistency, conditional-write, retention and cloud-durability assumptions.
5. Add guide/runbook/source mapping, scoped `make verify ID=C06-02`, CI artifacts and full regression checks. Update all 88 retained package IDs, commit/push, and verify exact-SHA CI.

This is one bounded lab increment. It does not replace an object store, supply distributed election, authorize scarce Inventory writes from snapshots, or establish cloud durability from emulator success. Full semantic/scale and cloud operations audits remain in the baseline.
