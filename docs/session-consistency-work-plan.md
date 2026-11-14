# C06-04 implementation increment

Date: 14 November 2026. Preserve the complete baseline under `docs/plan/`.

1. Add opt-in primary/replica read routing with signed tenant/source/history-bound WAL watermarks, bounded waits, sticky preference and explicit pending/primary fallback behavior. Keep command authority on the primary.
2. Integrate the contract with real Inventory stock/reservation reads and a single-transaction combined snapshot. Capture write fences after commit, preserve command receipts, and distinguish fence failure from failed business writes. Document the conservative future-read meaning of returned positions.
3. Provide a session client that retains its watermark across requests and handles missing write fences conservatively. Bound token/vector/header/cache/state sizes; reject tampering, wrong tenants, incompatible histories and unsupported read contracts.
4. Implement a deterministic lag/causality model and an actual primary/two-standby Atlas HTTP drill. Reproduce stale reads, read-your-writes and monotonic/prefix anomalies before testing remedies. Cover waits, timeout/fallback, sticky loss, endpoint failure, history changes and independent request concurrency.
5. Demonstrate causal dependencies and the limit of per-shard vectors: they do not establish an atomic cross-shard snapshot. Save guide/runbook/heading mappings and reports; run scoped/regression checks, retain all 88 packages, push and verify exact-SHA CI.

This is a bounded Inventory HTTP increment. Complete cross-context/transport routing, automatic election/history management, distributed snapshot coordination, production topology/role audits and broader semantic/scale work remain in the baseline.
