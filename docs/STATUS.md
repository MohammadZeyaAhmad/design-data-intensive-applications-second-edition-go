# Implementation status

Updated 10 November 2026. The saved plan under `docs/plan/` is the original design baseline. This file records the actual implementation. Full book coverage remains the final release gate.

## First working increment

Implemented: a Go API, separate durable projection worker, PostgreSQL migrations, inventory aggregates and vertical slices, tenant-bound development credentials, transactional request idempotency, outbox/inbox, ordered projections, poison-event quarantine, a guide CLI, Compose, CI configuration, meaningful concurrency/failure tests, and one lost-update lab.

These pieces begin several large work packages; they do not complete those packages in full.

| Plan package | Current status | Remaining scope |
|---|---|---|
| APP-01 | In progress: modules, local commands, source inventory, Compose, CI | Full guide/scenario manifests, deterministic harness, all profiles |
| APP-02 | In progress: Inventory domain, feature boundaries and adapter separation | Other bounded contexts, cross-context contracts, full domain map |
| APP-03 | In progress: receive/reserve/commit/release and contention checks | Expiration, compensating allocation reversal, full lifecycle |
| APP-06 | In progress: PostgreSQL outbox/dispatcher/inbox and per-stream quarantine | Kafka publication, broker crash boundaries, general replay/repair tooling |
| APP-08 | In progress: input contracts, tenant isolation, request identity | OIDC, authorization roles, pagination and full API contracts |
| C03-06 | In progress: asynchronous inventory read model | Event-sourced Ordering, complete CQRS views and online generation rebuild |
| C08-01 | In progress: real local transactions and atomic rollback evidence | Full ACID/durability/ambiguous-commit comparisons |
| C08-03 | In progress: lost-update model and actual locked inventory updates | Atomic/CAS/replicated variants and comparative benchmarks |
| C08-08 | In progress: local inbox effect deduplication | Full XA/acknowledgment comparison and external-effect scenarios |
| Remaining packages | Planned | See the complete backlog; none is silently removed |

## Next implementation sequence

1. Complete inventory expiry and compensation, with cancellation/expiry/commit race tests.
2. Add Catalog and Ordering boundaries; implement the PostgreSQL event store with expected-version append, replay, snapshots, and upcasters.
3. Add checkout process manager, payment simulator, immutable balanced journal, and reconciliation.
4. Add Kafka as a real transport adapter and explicit commit/publish/acknowledgment crash tests.
5. Continue the chapter-by-chapter implementation with the saved coverage inventory and acceptance criteria.

Payment processing, Kubernetes deployment, backup/restore workflows, Raft, CDC, and complete storage engines are not implemented here yet. Current capabilities are defined by the source and recorded validation, not by the eventual project scope.
