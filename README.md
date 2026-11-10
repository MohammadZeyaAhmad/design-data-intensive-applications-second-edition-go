# Atlas — Designing Data-Intensive Applications in Go

A practical companion to **Designing Data-Intensive Applications, second edition** by Martin Kleppmann and Chris Riccomini. The goal is one cohesive Go backend and a complete set of connected data-system experiments, with equal emphasis on learning and production engineering.

**Implementation is in progress.** Working code includes Catalog quotes, transactional Inventory, event-sourced Ordering, a durable checkout process manager, and a simulated provider with an immutable payment journal, warehouse inventory, durable Fulfillment, versioned provenance, audited local workflow controls, an optional real Kafka transport, a Temporal checkout alternative, and persistent LSM and B+ tree storage labs. The [complete plan](docs/plan/ddia-go-mega-project-plan.md), [88-package backlog](docs/plan/implementation-backlog.md), and [current status](docs/STATUS.md) distinguish the eventual scope from working code.

## Run the working implementation

Prerequisites: Go 1.27+, Docker with Compose, and GNU Make. The verified toolchain is Go 1.27.3. Default ports are **18081** for HTTP and **15433** for PostgreSQL; both bind to loopback. Commands run from the repository root.

```sh
make doctor
make up
make demo
make demo-orders
make demo-checkout
make verify-fulfillment
make verify
make down
```

`make up` generates random local credentials in ignored `.env` (mode 0600), compiles a static Go binary, builds its container image, starts pinned PostgreSQL, runs checksum-validated migrations, and starts the API, workers and a separate simulated provider. Existing credentials are preserved; newly required local settings are added without printing them. `make down` retains database volumes.

`make demo` receives ten units, reserves two, and commits that reservation using the real API. `make demo-orders` places and cancels an order intent. `make verify` runs domain/HTTP/architecture checks, Go race detection, real PostgreSQL integration tests, the lost-update lab, source/progress checks, live contention, expiry/reversal, event-store operations, warehouse shipment handoff, lineage/operator recovery, and worker/API restart recovery. Recovery verification stops/restarts only this project's local `ddia-go` services; run it on a development stack. Generated reports are in ignored `artifacts/`.

## Implemented behavior

- Inventory aggregate with explicit available/reserved/allocated invariants and bounded quantities.
- Vertical slices for stock receipts, reservations, commitment, release, and explicit allocation reversal.
- Database-clock expiry that cannot authorize a late commit after a lock wait, with a durable expiry worker.
- Tenant identity bound to a development bearer credential; callers cannot select another tenant with a header or body field.
- PostgreSQL row locking and optimistic version checks, with composite tenant keys and database constraints.
- Transactional request idempotency: concurrent duplicate requests return one saved outcome; changed payloads conflict.
- Transactional outbox and a separate worker that maintains an asynchronous inventory read model.
- Per-SKU event ordering, inbox deduplication, bounded retries, and quarantine that blocks only the affected stream.
- Checksum-protected, transactionally locked migrations; graceful process shutdown and readiness checks.
- Event-sourced Ordering with immutable events, expected-version append, deterministic replay, and transactional outbox/request results.
- Versioned snapshots with checksum validation and full-replay fallback, plus a legacy placement-event upcaster.
- Separate order read models, keyset event-history pagination, and a stream-scoped rebuild barrier compatible with live appends.
- Operator commands for order snapshots and projection reconstruction; independent context boundaries enforced by tests.
- Immutable Catalog price versions and quotes; checkout freezes server-owned amount/currency.
- Durable checkout claims/checkpoints, compensation, source outcome fences and resource-close markers.
- A separate authenticated provider simulator, unknown-response reconciliation and immutable balanced capture/refund transfers.
- Warehouse-scoped stock, reservations and ordered projections with legacy default-location compatibility.
- Confirmed-order Fulfillment requests, one shipment per warehouse, versioned operator commands and replay-safe physical handoff.
- Actor/correlation/causation/origin-request metadata persisted across asynchronous work, with explicit legacy provenance.
- Tenant-scoped local pause/resume/retry controls, live-lease protection and immutable transactional control audit.
- Optional Kafka profile for events and workflow requests, with durable staging, manual offsets, ordered owner receipts, poison-stream isolation and audited retry.
- Optional Temporal checkout profile with immutable ownership, atomic activity receipts, durable timers/signals, cancellation compensation and compatible history replay.
- Persistent Go LSM lab with WAL/SSTables, sparse indexes, Bloom filters, two compaction policies, durable manifests, modeled power loss and real process kills; compared with bbolt.
- Custom page-based B+ tree with splits/merges, persistent free-page reuse, full-page redo, structural validation and interrupted-recovery tests.
- Independent Go lab showing a lost-update schedule, connected to the real database contention tests.

Inventory state is authoritative in PostgreSQL tables; Ordering state is derived from its immutable event stream. A placed order currently records SKU/quantity intent. An order remains unpriced intent until checkout accepts a Catalog quote; captured payment and committed stock are required for confirmation. The local dispatcher uses PostgreSQL. Catalog price authority, checkout capture/compensation and a two-leg payment journal now run through published contracts; warehouse sourcing and a separate Fulfillment context now carry confirmed allocations through picking, packing, durable dispatch and delivery recording. Kafka transport is now executable; CDC, replicated broker operation, full telemetry and the remaining laboratories are tracked in the backlog.

## Repository map

```text
cmd/atlas/                         API, worker, migration, and schema-check commands
cmd/guide/                         Local configuration, demos, verification, source inventory
cmd/admin/                         Order snapshot/rebuild and audited local workflow controls
internal/inventory/domain/         Pure aggregate and state-machine rules
internal/inventory/features/       Use cases grouped by feature
internal/inventory/ports/          Tenant-scoped transaction and query interfaces
internal/inventory/adapters/       PostgreSQL and HTTP implementations
internal/ordering/                 Event-sourced aggregate, feature handlers, ports and adapters
internal/catalog/                  Versioned price authority and immutable quotes
internal/checkout/                 Durable process manager using published contracts
internal/payments/                 Provider adapter, reconciliation and financial journal
internal/fulfillment/              Warehouse shipments, immutable history and durable handoff
contracts/                         Published business interfaces and value representations
internal/platform/                Configuration and identifier utilities
migrations/                       Embedded, checksum-validated SQL
labs/                             Separate Go module: storage engines, embedded comparison, lost-update lab
tests/architecture/                Enforced domain/feature import boundaries
tests/integration/                 Real PostgreSQL concurrency and failure tests
scenarios/                        Named verification scenarios linked to the plan
deploy/compose/                    Reproducible local services
docs/                             Guide, decisions, status, and full-book plan
```

Start with the [inventory API](docs/API.md), [Ordering API](docs/ORDERING_API.md), [commerce contracts](docs/COMMERCE_API.md), [event-sourcing walkthrough](docs/chapters/ch03/event-sourcing.md), [checkout walkthrough](docs/chapters/ch05/checkout-workflow.md), and [Fulfillment contracts](docs/FULFILLMENT_API.md), and [provenance/operator controls](docs/OPERATIONS.md), and [Kafka transport](docs/MESSAGING.md), and [Temporal durable execution](docs/TEMPORAL.md), and [storage engines](docs/STORAGE.md), and the [B+ tree](docs/BTREE.md). See [validation](docs/VALIDATION.md) for measured evidence and limitations. Machine-readable implementation progress is in [docs/progress.json](docs/progress.json).

## Full-book scope remains mandatory

The planning inventory covers **14 chapters, 476 substantive headings, 61 glossary terms, 4,215 index occurrences, and 48 backend-pattern entries**. Every concept requires an implementation, executable experiment/model, or appropriate worked design exercise. Runtime profiles limit resources, not final scope.

```sh
make coverage-check     # validates the saved source inventory, not completeness of code
make lab                # runs the deterministic lost-update demonstration
make verify-storage     # LSM/map oracle, power-loss model, real kills and bbolt comparison
make verify ID=C04-01    # scoped verification of the LSM package
make verify-btree       # B+ tree structure, redo/crash tests and bbolt comparison
make verify ID=C04-02    # scoped verification of the B+ tree package
make storage-demo       # five-engine warehouse comparison; writes artifacts/storage.json
make integration        # fails if the local atlas_test binding is missing
make verify-lifecycle   # expiry, late commit and allocation reversal over real HTTP
make verify-orders      # expected-version race, snapshot, rebuild and event history
make verify-checkout    # price authority, capture uncertainty and compensation
make verify-checkout-recovery # provider outage and worker restart
make verify-fulfillment # two warehouses, shipment commands and physical stock
make verify-fulfillment-recovery # durable dispatch across API/worker restart
make verify-operations # lineage and audited pause/restart/resume using the real CLI
make up-kafka         # optional Kafka transport profile; stop the other profile first
make verify-kafka     # broker outage, publisher kill, duplicate/conflict recovery
make integration-kafka # real Kafka acknowledgments and consumer-group rebalance
make down-kafka       # stops this profile and retains database/broker volumes
make up-temporal      # optional workflow engine; stop the other profile first
ATLAS_CHECKOUT_ENGINE=temporal make verify-checkout verify-fulfillment verify-operations
make verify-temporal  # server/worker SIGKILL, persisted history and payment recovery
make integration-temporal # real server, activity-stage restarts and v0 replay
make down-temporal    # retains PostgreSQL and Temporal history volumes
```

The original plan is preserved under `docs/plan/`; [docs/STATUS.md](docs/STATUS.md) is the current implementation record. The EPUB and extracted book prose are intentionally not part of this repository.

Integration tests connect to the local `atlas_test` administrative binding, create a randomly named `atlas_test_run_*` database, and drop only that database afterward. The test role needs `CREATE DATABASE` permission; this avoids deleting or rewriting immutable historical events between test runs.

The Temporal profile currently uses PostgreSQL message delivery; the combined Kafka/Temporal profile is not certified. An accepted checkout retains its engine, so drain pending Temporal work before stopping that runtime.

For an existing earlier-increment checkout, upgrade with `docker compose --env-file .env -f deploy/compose/compose.yaml -p ddia-go stop api worker provider`, then `make up`. This preserves the database and applies the new migrations before starting updated binaries. Existing holds receive a fifteen-minute grace period at the expiry migration. The warehouse migration maps existing stock/events to `default` and schedules ready Fulfillment requests for previously confirmed orders. The expanded projection key requires the updated API/worker pair; use a coordinated local upgrade.

## Development boundary

This increment is for local development and learning. The two static tenant keys are a bootstrap authentication mechanism, not OIDC. Database connections in the Compose profile use a dedicated local network without TLS, and the scratch image has no general-purpose CA bundle. Production identity, tenant RLS, remote TLS trust, infrastructure, operational hardening, and full recovery certification remain planned. No real payment account or cloud account is required.

The supplied source currently has no open-source license grant; license selection remains a repository-owner decision.
