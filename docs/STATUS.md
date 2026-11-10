# Implementation status

Updated 10 November 2026. The saved plan under `docs/plan/` is the original design baseline. This file records the actual implementation. Full book coverage remains the final release gate.

## Working implementation

Implemented: a Go API, inventory and Ordering contexts, separate durable workers, PostgreSQL migrations, tenant-bound authentication, transactional request identity, outbox/inbox, ordered projections, quarantine, a guide CLI, Compose, CI, concurrency/failure tests, and a lost-update lab.

The second increment adds database-clock reservation expiry, explicit allocation reversal, an immutable order event store, expected-version commands, full replay, legacy-event upcasting, checksum-validated snapshots, keyset history, asynchronous order views, and a per-stream rebuild that can run alongside appends. Both contexts recover through worker/API restarts. That increment established 19 real PostgreSQL integration tests.

The third increment adds Catalog price versions/quotes, explicit published contracts, a durable checkout process manager, an authenticated HTTP provider simulator and a balanced immutable capture/refund journal. Resource-close markers fence delayed reserve/capture calls; source outcome facts fence stale confirmation/compensation. The full suite now has 30 real PostgreSQL integration tests plus provider-outage/worker-restart HTTP verification.

The fourth increment adds warehouse-scoped stock and event ordering, pinned checkout sourcing, a separate Fulfillment context, durable confirmation dispatch, immutable shipment history, expected-version warehouse commands and idempotent physical stock handoff. The suite now has **42 real PostgreSQL integration tests**, plus real HTTP/API/worker-restart shipment scenarios.

The fifth increment adds versioned actor/correlation/cause/origin-request envelopes across all six contexts and the provider, server-owned request identity, immutable origin/dispatch/cancellation provenance, and tenant-scoped local workflow controls. Pause/resume/retry preserves business progress and resource IDs, checks live leases, and atomically records an immutable audit and saved response. The suite now has **55 real PostgreSQL integration tests**, plus a CLI/HTTP pause-restart-resume and lineage scenario.

The sixth increment adds a pinned optional Kafka profile for four delivery lanes, separate publication cursors, a durable transport inbox, manual offset acknowledgment, fixed partition keys, source validation, stream-scoped quarantine and audited retry. The **65 PostgreSQL integration tests** are supplemented by **three isolated real-Kafka tests** for acknowledgment loss, durable-stage checkpoint loss, database-stage failure and consumer-group rebalance. Existing business flows and the broker outage/publisher-kill drill run through Kafka.

The seventh increment adds a real Temporal checkout adapter using the same business feature, immutable engine ownership, atomic activity result receipts, durable control wake signals, native cancellation compensation, bounded workflow histories and supported versioning. The suite now has **71 PostgreSQL integration tests**, **three real-Kafka tests**, and **three real-Temporal tests**, including worker replacement after five external stages and replay of an actual pre-version-marker history. Both checkout engines pass the same HTTP business outcome suite. A real server/worker SIGKILL drill preserves run identity and recovers an in-flight payment after natural lease expiry.

The eighth increment adds a persistent Go LSM storage lab with checksummed WAL framing, sorted immutable runs, sparse indexes, Bloom filters, tombstones, size-tiered and leveled compaction, atomic manifests, sync/async durability fences and a single-writer lock. Map-oracle tests, an independent power-loss filesystem model and **21 real process-kill boundaries** verify recovery. A synthetic warehouse workload compares the append-log baseline, both policies and **bbolt 1.4.3**, with repeated timing/space reports. Production contexts retain PostgreSQL authority.

The ninth increment adds the custom page-based B+ tree: bounded CRC-protected pages, exact separator routing, splits/borrows/merges, root collapse, linked-leaf ranges, persistent free-page reuse and full-page redo with restartable recovery. Reference/occupancy tests, modeled torn writes, initialization faults and **18 native checkpoint/recovery cases** verify the implementation. The shared warehouse runner now compares **five stores**; the dedicated tree report also deletes, reopens and reinserts the live set to verify allocation reuse. Common key/value types, file I/O and the fault model are shared across the independent storage labs.

These pieces begin several large work packages; they do not complete those packages in full.

| Plan package | Current status | Remaining scope |
|---|---|---|
| APP-01 | In progress: modules, local commands, source inventory, Compose, CI | Full guide/scenario manifests, deterministic harness, all profiles |
| APP-02 | In progress: Six private contexts and published warehouse/Fulfillment contracts; shared transport/auth and import checks | Full domain map, warehouse registry, production authorization and remaining context features |
| APP-03 | In progress: warehouse receive/reserve/commit/release/expire/reverse/ship; post-lock deadlines and independent streams | Single-SKU splits, transfers and broader policies |
| APP-04 | In progress: event append/replay, snapshots, upcasters, immutable source, request identity and versioned provenance | Broader lifecycle/archival policies, production actor identities and compatibility rollout |
| APP-05 | In progress: idempotent simulated capture/void/refund, unknown outcomes, immutable journal | Authorization separation, partial refunds, webhooks and real-provider adapter |
| APP-07 | In progress: durable checkout, leased steps, compensation, cancellation, outcome fences, warehouse handoff, audited controls and a Temporal alternative | Returns/post-confirmation changes, generalized business repair and production workflow operation |
| APP-06 | In progress: PostgreSQL/Kafka outbox publication, durable staging, manual offsets, owner receipts, per-stream quarantine and audited retry | Leased/CDC publication comparisons, broader broker operation and replay/repair tooling |
| APP-08 | In progress: input contracts, tenant isolation, server-owned request metadata and scoped local operator capability | OIDC, authorization roles, pagination and full API contracts |
| C03-06 | In progress: authoritative event-sourced commands and asynchronous order/inventory views | Full dashboard/fulfillment views and all-stream generation cutover |
| C05-03 | In progress: additive migrations and legacy placement upcasting | Mixed-version rollout/backfill/contract compatibility suite |
| C12-04 | In progress: immutable order facts and deterministic replay | Full retention/archive/privacy policies and CDC comparisons |
| C13-02 | In progress: one-order rebuild, snapshot fallback and live resumption | Shadow generations and batch/stream/all-stream convergence |
| C09-04 | In progress: job checkpoint tokens, resource-close fences and lease-aware operator controls | General coordination service/consensus-backed lease experiments |
| C13-05 | In progress: persisted lineage, dispatch redelivery, provider reconciliation, resource closure and audited controls | Broader replay horizons and transport comparisons |
| C08-01 | In progress: real local transactions and atomic rollback evidence | Full ACID/durability/ambiguous-commit comparisons |
| C08-03 | In progress: lost-update model and actual locked inventory updates | Atomic/CAS/replicated variants and comparative benchmarks |
| C08-08 | In progress: atomic owner receipts across database/broker acknowledgment boundaries | Full XA/acknowledgment comparisons and broader external-effect scenarios |
| C12-01 | In progress: real durable broker delivery, duplicate/poison handling and bounded receipt/application boundaries | Queue/broker models, routing/fan-out, expiry/backpressure comparisons |
| C12-02 | In progress: pinned Kafka profile, partition mapping, consumer groups, acknowledgment loss, rebalance and process recovery | Segmented-log broker, retention/lag/disk experiments, replicated durability and offset migration |
| C04-02 | In progress: custom B+ tree, page/structure checks, splits/merges, persistent free list, full-page redo and repeated-crash evidence | Full sub-concept audit, overflow values, denser layout and broader format/scale/concurrency variants |
| C04-01 | In progress: append log, WAL/SSTables, sparse/Bloom indexes, two policies, durable manifest recovery, model/process fault tests and mature-engine comparison | Full sub-concept audit, compression and broader format/scale stress |
| C05-05 | In progress: real Temporal workflow/activities, durable timers/signals, result receipts, versioned replay and restart evidence | Production deployment/retention, history repair and worker deployment routing |
| Remaining packages | Planned | See the complete backlog; none is silently removed |

## Next implementation sequence

1. Implement controlled storage/index amplification comparisons, then deepen format, compression and scale work.
2. Continue data models, replication, sharding, analytics and governance work against the full source inventory; deepen remaining application/operations work alongside them.

[Machine-readable progress](progress.json) retains all 88 package IDs. The [inventory lifecycle](../scenarios/inventory-lifecycle.json) and [Ordering](../scenarios/ordering-events.json) scenario manifests, together with [checkout/payments](../scenarios/checkout-payments.json), [warehouse/Fulfillment](../scenarios/warehouse-fulfillment.json), [workflow operations](../scenarios/workflow-operations.json), [Kafka transport](../scenarios/kafka-transport.json), [Temporal checkout](../scenarios/temporal-checkout.json), [storage/LSM](../scenarios/storage-lsm.json), and [B+ tree](../scenarios/storage-btree.json), record verified behaviors and evidence paths; they do not declare the much larger associated packages complete. `make coverage-check` validates their package IDs and file references in addition to the book inventory.

Real payment/carrier-network integration, returns, single-SKU warehouse splits, Kubernetes deployment, backup/restore workflows, Raft, CDC, and complete storage engines remain unfinished. Current capabilities are defined by the source and recorded validation, not by the eventual project scope.
