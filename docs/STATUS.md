# Implementation status

Updated 10 November 2026. The saved plan under `docs/plan/` is the original design baseline. This file records the actual implementation. Full book coverage remains the final release gate.

## Working implementation

Implemented: a Go API, inventory and Ordering contexts, separate durable workers, PostgreSQL migrations, tenant-bound authentication, transactional request identity, outbox/inbox, ordered projections, quarantine, a guide CLI, Compose, CI, concurrency/failure tests, and a lost-update lab.

The second increment adds database-clock reservation expiry, explicit allocation reversal, an immutable order event store, expected-version commands, full replay, legacy-event upcasting, checksum-validated snapshots, keyset history, asynchronous order views, and a per-stream rebuild that can run alongside appends. Both contexts recover through worker/API restarts. That increment established 19 real PostgreSQL integration tests.

The third increment adds Catalog price versions/quotes, explicit published contracts, a durable checkout process manager, an authenticated HTTP provider simulator and a balanced immutable capture/refund journal. Resource-close markers fence delayed reserve/capture calls; source outcome facts fence stale confirmation/compensation. The full suite now has 30 real PostgreSQL integration tests plus provider-outage/worker-restart HTTP verification.

The fourth increment adds warehouse-scoped stock and event ordering, pinned checkout sourcing, a separate Fulfillment context, durable confirmation dispatch, immutable shipment history, expected-version warehouse commands and idempotent physical stock handoff. The suite now has **42 real PostgreSQL integration tests**, plus real HTTP/API/worker-restart shipment scenarios.

The fifth increment adds versioned actor/correlation/cause/origin-request envelopes across all six contexts and the provider, server-owned request identity, immutable origin/dispatch/cancellation provenance, and tenant-scoped local workflow controls. Pause/resume/retry preserves business progress and resource IDs, checks live leases, and atomically records an immutable audit and saved response. The suite now has **55 real PostgreSQL integration tests**, plus a CLI/HTTP pause-restart-resume and lineage scenario.

These pieces begin several large work packages; they do not complete those packages in full.

| Plan package | Current status | Remaining scope |
|---|---|---|
| APP-01 | In progress: modules, local commands, source inventory, Compose, CI | Full guide/scenario manifests, deterministic harness, all profiles |
| APP-02 | In progress: Six private contexts and published warehouse/Fulfillment contracts; shared transport/auth and import checks | Full domain map, warehouse registry, production authorization and remaining context features |
| APP-03 | In progress: warehouse receive/reserve/commit/release/expire/reverse/ship; post-lock deadlines and independent streams | Single-SKU splits, transfers and broader policies |
| APP-04 | In progress: event append/replay, snapshots, upcasters, immutable source, request identity and versioned provenance | Broader lifecycle/archival policies, production actor identities and compatibility rollout |
| APP-05 | In progress: idempotent simulated capture/void/refund, unknown outcomes, immutable journal | Authorization separation, partial refunds, webhooks and real-provider adapter |
| APP-07 | In progress: durable checkout, leased steps, compensation, cancellation, outcome fences, warehouse handoff and audited scheduling controls | Returns/post-confirmation changes, generalized business repair and durable-execution alternative |
| APP-06 | In progress: PostgreSQL outbox/dispatcher/inbox and per-stream quarantine | Kafka publication, broker crash boundaries, general replay/repair tooling |
| APP-08 | In progress: input contracts, tenant isolation, server-owned request metadata and scoped local operator capability | OIDC, authorization roles, pagination and full API contracts |
| C03-06 | In progress: authoritative event-sourced commands and asynchronous order/inventory views | Full dashboard/fulfillment views and all-stream generation cutover |
| C05-03 | In progress: additive migrations and legacy placement upcasting | Mixed-version rollout/backfill/contract compatibility suite |
| C12-04 | In progress: immutable order facts and deterministic replay | Full retention/archive/privacy policies and CDC comparisons |
| C13-02 | In progress: one-order rebuild, snapshot fallback and live resumption | Shadow generations and batch/stream/all-stream convergence |
| C09-04 | In progress: job checkpoint tokens, resource-close fences and lease-aware operator controls | General coordination service/consensus-backed lease experiments |
| C13-05 | In progress: persisted lineage, dispatch redelivery, provider reconciliation, resource closure and audited controls | Broader replay horizons and transport comparisons |
| C08-01 | In progress: real local transactions and atomic rollback evidence | Full ACID/durability/ambiguous-commit comparisons |
| C08-03 | In progress: lost-update model and actual locked inventory updates | Atomic/CAS/replicated variants and comparative benchmarks |
| C08-08 | In progress: local inbox effect deduplication | Full XA/acknowledgment comparison and external-effect scenarios |
| Remaining packages | Planned | See the complete backlog; none is silently removed |

## Next implementation sequence

1. Add Kafka as a transport adapter and explicit broker publish/acknowledgment failure scenarios.
2. Implement the Temporal workflow alternative and compare it with the current persisted process manager.
3. Continue storage, replication, sharding, analytics and governance work against the full source inventory.

[Machine-readable progress](progress.json) retains all 88 package IDs. The [inventory lifecycle](../scenarios/inventory-lifecycle.json) and [Ordering](../scenarios/ordering-events.json) scenario manifests, together with [checkout/payments](../scenarios/checkout-payments.json), [warehouse/Fulfillment](../scenarios/warehouse-fulfillment.json), and [workflow operations](../scenarios/workflow-operations.json), record verified behaviors and evidence paths; they do not declare the much larger associated packages complete. `make coverage-check` validates their package IDs and file references in addition to the book inventory.

Real payment/carrier-network integration, returns, single-SKU warehouse splits, Kubernetes deployment, backup/restore workflows, Raft, CDC, and complete storage engines remain unfinished. Current capabilities are defined by the source and recorded validation, not by the eventual project scope.
