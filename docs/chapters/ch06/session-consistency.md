# Replication lag and session consistency

| Book section | Executable evidence |
| --- | --- |
| B06-013 — Problems with Replication Lag | Real acknowledged Inventory receipt plus independently paused PostgreSQL standbys; raw stale reads precede guarded HTTP reads |
| B06-014 — Reading your own writes | Postcommit signed WAL fence, primary reads, bounded replay wait and explicit pending/fallback; saved receipts survive token-capture failure |
| B06-016 — Monotonic reads | Alternating native replicas regress 2→1; retained session watermark and a serializing Go client prevent accepting replay below that fence |
| B06-017 — Consistent prefix reads | Native torn stock/reservation view followed by one RR snapshot; causal-dependency vector model includes cross-shard parent re-read |
| B06-018 — Solutions for Replication Lag | Explicit read contract, sticky preference with safe relocation, bounded waits, primary fallback, tenant/source/history-bound tokens and cancellation |
| B06-038 — Monitoring staleness | Actual native primary/replay byte-gap samples plus bounded request-driven probe/fence-gap/timestamp and pending/fallback metrics on the private telemetry endpoint |

Pattern **P30**, “Replication, session consistency, and read routing,” is implemented through the [production router](../../../internal/platform/readrouting), [Inventory adapter](../../../internal/inventory/adapters/postgres/session.go), [HTTP contract](../../../internal/inventory/adapters/httpapi/session.go), [native drill/model](../../../labs/ch06/session-consistency) and [database regressions](../../../tests/integration/session_consistency_test.go). The token is a future-read fence and does not name an exact snapshot. Strict reads depend on the admitted primary history.

The [guide](../../SESSION_CONSISTENCY.md), [scenario](../../../scenarios/session-consistency.json) and [validation](../../VALIDATION.md) retain the distinction between native single-shard evidence, modeled cross-shard causality and an unimplemented distributed atomic snapshot. Full semantic/scale/role audits and all remaining package obligations are preserved under `docs/plan/`.
