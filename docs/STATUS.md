# Implementation status

Updated 10 November 2026. The saved plan under `docs/plan/` is the original design baseline. This file records the actual implementation. Full book coverage remains the final release gate.

## Working implementation

Implemented: a Go API, inventory and Ordering contexts, separate durable workers, PostgreSQL migrations, tenant-bound authentication, transactional request identity, outbox/inbox, ordered projections, quarantine, a guide CLI, Compose, CI, concurrency/failure tests, and a lost-update lab.

The second increment adds database-clock reservation expiry, explicit allocation reversal, an immutable order event store, expected-version commands, full replay, legacy-event upcasting, checksum-validated snapshots, keyset history, asynchronous order views, and a per-stream rebuild that can run alongside appends. Both contexts recover through worker/API restarts. That increment established 19 real PostgreSQL integration tests.

The third increment adds Catalog price versions/quotes, explicit published contracts, a durable checkout process manager, an authenticated HTTP provider simulator and a balanced immutable capture/refund journal. Resource-close markers fence delayed reserve/capture calls; source outcome facts fence stale confirmation/compensation. The full suite now has 30 real PostgreSQL integration tests plus provider-outage/worker-restart HTTP verification.

These pieces begin several large work packages; they do not complete those packages in full.

| Plan package | Current status | Remaining scope |
|---|---|---|
| APP-01 | In progress: modules, local commands, source inventory, Compose, CI | Full guide/scenario manifests, deterministic harness, all profiles |
| APP-02 | In progress: Inventory and Ordering boundaries; shared transport/auth; import checks | Fulfillment and warehouse contracts; full domain map and production authorization |
| APP-03 | Lifecycle verified: receive/reserve/commit/release/expire/reverse; post-lock deadline check | Warehouse dimension and fulfillment integration still need the broader domain contracts |
| APP-04 | In progress: event append/replay, snapshots, upcasters, immutable source, expected version and request identity | Complete checkout lifecycle events, full actor/correlation envelope, archival lifecycle |
| APP-05 | In progress: idempotent simulated capture/void/refund, unknown outcomes, immutable journal | Authorization separation, partial refunds, webhooks and real-provider adapter |
| APP-07 | In progress: durable multi-line checkout, leased steps, compensation, cancellation and outcome fences | Fulfillment, generalized manual recovery, durable-execution alternative |
| APP-06 | In progress: PostgreSQL outbox/dispatcher/inbox and per-stream quarantine | Kafka publication, broker crash boundaries, general replay/repair tooling |
| APP-08 | In progress: input contracts, tenant isolation, request identity | OIDC, authorization roles, pagination and full API contracts |
| C03-06 | In progress: authoritative event-sourced commands and asynchronous order/inventory views | Full dashboard/fulfillment views and all-stream generation cutover |
| C05-03 | In progress: additive migrations and legacy placement upcasting | Mixed-version rollout/backfill/contract compatibility suite |
| C12-04 | In progress: immutable order facts and deterministic replay | Full retention/archive/privacy policies and CDC comparisons |
| C13-02 | In progress: one-order rebuild, snapshot fallback and live resumption | Shadow generations and batch/stream/all-stream convergence |
| C09-04 | In progress: job checkpoint tokens and resource-close fences | General coordination service/consensus-backed lease experiments |
| C13-05 | In progress: end-to-end IDs, dispatch redelivery, provider reconciliation and resource closure | Broader replay horizons and transport comparisons |
| C08-01 | In progress: real local transactions and atomic rollback evidence | Full ACID/durability/ambiguous-commit comparisons |
| C08-03 | In progress: lost-update model and actual locked inventory updates | Atomic/CAS/replicated variants and comparative benchmarks |
| C08-08 | In progress: local inbox effect deduplication | Full XA/acknowledgment comparison and external-effect scenarios |
| Remaining packages | Planned | See the complete backlog; none is silently removed |

## Next implementation sequence

1. Add warehouse-aware inventory and the Fulfillment context, preserving checkout compensation ownership.
2. Complete actor/correlation envelopes and generalized operational recovery controls.
3. Add Kafka as a transport adapter and explicit broker publish/acknowledgment failure scenarios.
4. Implement the Temporal workflow alternative and compare it with the current persisted process manager.
5. Continue storage, replication, sharding, analytics and governance work against the full source inventory.

[Machine-readable progress](progress.json) retains all 88 package IDs. The [inventory lifecycle](../scenarios/inventory-lifecycle.json) and [Ordering](../scenarios/ordering-events.json) scenario manifests, together with [checkout/payments](../scenarios/checkout-payments.json), record verified behaviors and evidence paths; they do not declare the much larger associated packages complete. `make coverage-check` validates their package IDs and file references in addition to the book inventory.

Real payment-network integration, Fulfillment/shipping, Kubernetes deployment, backup/restore workflows, Raft, CDC, and complete storage engines remain unfinished. Current capabilities are defined by the source and recorded validation, not by the eventual project scope.
