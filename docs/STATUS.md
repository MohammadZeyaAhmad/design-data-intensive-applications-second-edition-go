# Implementation status

Updated 10 November 2026. The saved plan under `docs/plan/` is the original design baseline. This file records the actual implementation. Full book coverage remains the final release gate.

## Working implementation

Implemented: a Go API, inventory and Ordering contexts, separate durable workers, PostgreSQL migrations, tenant-bound authentication, transactional request identity, outbox/inbox, ordered projections, quarantine, a guide CLI, Compose, CI, concurrency/failure tests, and a lost-update lab.

The second increment adds database-clock reservation expiry, explicit allocation reversal, an immutable order event store, expected-version commands, full replay, legacy-event upcasting, checksum-validated snapshots, keyset history, asynchronous order views, and a per-stream rebuild that can run alongside appends. Both contexts recover through worker/API restarts. There are now 19 real PostgreSQL integration tests.

These pieces begin several large work packages; they do not complete those packages in full.

| Plan package | Current status | Remaining scope |
|---|---|---|
| APP-01 | In progress: modules, local commands, source inventory, Compose, CI | Full guide/scenario manifests, deterministic harness, all profiles |
| APP-02 | In progress: Inventory and Ordering boundaries; shared transport/auth; import checks | Catalog/Payments/Fulfillment, published integration contracts, full domain map |
| APP-03 | Lifecycle verified: receive/reserve/commit/release/expire/reverse; post-lock deadline check | Warehouse dimension and fulfillment integration still need the broader domain contracts |
| APP-04 | In progress: event append/replay, snapshots, upcasters, immutable source, expected version and request identity | Complete checkout lifecycle events, full actor/correlation envelope, archival lifecycle |
| APP-06 | In progress: PostgreSQL outbox/dispatcher/inbox and per-stream quarantine | Kafka publication, broker crash boundaries, general replay/repair tooling |
| APP-08 | In progress: input contracts, tenant isolation, request identity | OIDC, authorization roles, pagination and full API contracts |
| C03-06 | In progress: authoritative event-sourced commands and asynchronous order/inventory views | Full dashboard/fulfillment views and all-stream generation cutover |
| C05-03 | In progress: additive migrations and legacy placement upcasting | Mixed-version rollout/backfill/contract compatibility suite |
| C12-04 | In progress: immutable order facts and deterministic replay | Full retention/archive/privacy policies and CDC comparisons |
| C13-02 | In progress: one-order rebuild, snapshot fallback and live resumption | Shadow generations and batch/stream/all-stream convergence |
| C08-01 | In progress: real local transactions and atomic rollback evidence | Full ACID/durability/ambiguous-commit comparisons |
| C08-03 | In progress: lost-update model and actual locked inventory updates | Atomic/CAS/replicated variants and comparative benchmarks |
| C08-08 | In progress: local inbox effect deduplication | Full XA/acknowledgment comparison and external-effect scenarios |
| Remaining packages | Planned | See the complete backlog; none is silently removed |

## Next implementation sequence

1. Add Catalog price/version authority and warehouse-aware inventory contracts.
2. Integrate the existing Ordering aggregate with checkout, a payment simulator, an immutable balanced journal, and reconciliation.
3. Extend lifecycle events/correlation and add Kafka as a real transport adapter with explicit publish/acknowledgment crash tests.
4. Add all-stream shadow projection generations, durable archival/replay policies and workflow execution alternatives.
5. Continue the chapter-by-chapter implementation against the saved source inventory.

[Machine-readable progress](progress.json) retains all 88 package IDs. The two [inventory lifecycle](../scenarios/inventory-lifecycle.json) and [Ordering](../scenarios/ordering-events.json) scenario manifests record verified behaviors and evidence paths; they do not declare the much larger associated packages complete. `make coverage-check` validates their package IDs and file references in addition to the book inventory.

Payment processing, Kubernetes deployment, backup/restore workflows, Raft, CDC, and complete storage engines are not implemented here yet. Current capabilities are defined by the source and recorded validation, not by the eventual project scope.
