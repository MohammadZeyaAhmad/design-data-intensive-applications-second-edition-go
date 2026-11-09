# Atlas — Designing Data-Intensive Applications in Go

A practical companion to **Designing Data-Intensive Applications, second edition** by Martin Kleppmann and Chris Riccomini. The goal is one cohesive Go backend and a complete set of connected data-system experiments, with equal emphasis on learning and production engineering.

**Implementation is in progress.** Working code includes transactional inventory with expiry and compensation, plus event-sourced Ordering with snapshots, schema evolution, and CQRS projections. The [complete plan](docs/plan/ddia-go-mega-project-plan.md), [88-package backlog](docs/plan/implementation-backlog.md), and [current status](docs/STATUS.md) distinguish the eventual scope from working code.

## Run the working implementation

Prerequisites: Go 1.27+, Docker with Compose, and GNU Make. The verified toolchain is Go 1.27.3. Default ports are **18081** for HTTP and **15433** for PostgreSQL; both bind to loopback. Commands run from the repository root.

```sh
make doctor
make up
make demo
make demo-orders
make verify
make down
```

`make up` generates random local credentials in ignored `.env` (mode 0600), compiles a static Go binary, builds its container image, starts pinned PostgreSQL, runs checksum-validated migrations, and starts the API and projection worker. Existing `.env` is preserved. `make down` retains database volumes.

`make demo` receives ten units, reserves two, and commits that reservation using the real API. `make demo-orders` places and cancels an order intent. `make verify` runs domain/HTTP/architecture checks, Go race detection, real PostgreSQL integration tests, the lost-update lab, source/progress checks, live contention, expiry/reversal, event-store operations, and worker/API restart recovery. Recovery verification stops/restarts only this project's local `ddia-go` services; run it on a development stack. Generated reports are in ignored `artifacts/`.

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
- Independent Go lab showing a lost-update schedule, connected to the real database contention tests.

Inventory state is authoritative in PostgreSQL tables; Ordering state is derived from its immutable event stream. A placed order currently records SKU/quantity intent. Catalog pricing, checkout, payment, shipment and cross-context compensation are subsequent milestones. The local dispatcher uses PostgreSQL; Kafka/CDC, full telemetry and the remaining laboratories are tracked in the backlog.

## Repository map

```text
cmd/atlas/                         API, worker, migration, and schema-check commands
cmd/guide/                         Local configuration, demos, verification, source inventory
cmd/admin/                         Scoped order snapshot and rebuild operations
internal/inventory/domain/         Pure aggregate and state-machine rules
internal/inventory/features/       Use cases grouped by feature
internal/inventory/ports/          Tenant-scoped transaction and query interfaces
internal/inventory/adapters/       PostgreSQL and HTTP implementations
internal/ordering/                 Event-sourced aggregate, feature handlers, ports and adapters
internal/platform/                Configuration and identifier utilities
migrations/                       Embedded, checksum-validated SQL
labs/                             Separate Go module for educational implementations
tests/architecture/                Enforced domain/feature import boundaries
tests/integration/                 Real PostgreSQL concurrency and failure tests
scenarios/                        Named verification scenarios linked to the plan
deploy/compose/                    Reproducible local services
docs/                             Guide, decisions, status, and full-book plan
```

Start with the [inventory API](docs/API.md), [Ordering API](docs/ORDERING_API.md), and [event-sourcing walkthrough](docs/chapters/ch03/event-sourcing.md). See [validation](docs/VALIDATION.md) for measured evidence and limitations. Machine-readable implementation progress is in [docs/progress.json](docs/progress.json).

## Full-book scope remains mandatory

The planning inventory covers **14 chapters, 476 substantive headings, 61 glossary terms, 4,215 index occurrences, and 48 backend-pattern entries**. Every concept requires an implementation, executable experiment/model, or appropriate worked design exercise. Runtime profiles limit resources, not final scope.

```sh
make coverage-check     # validates the saved source inventory, not completeness of code
make lab                # runs the first deterministic lost-update demonstration
make integration        # fails if the local atlas_test binding is missing
make verify-lifecycle   # expiry, late commit and allocation reversal over real HTTP
make verify-orders      # expected-version race, snapshot, rebuild and event history
```

The original plan is preserved under `docs/plan/`; [docs/STATUS.md](docs/STATUS.md) is the current implementation record. The EPUB and extracted book prose are intentionally not part of this repository.

Integration tests connect to the local `atlas_test` administrative binding, create a randomly named `atlas_test_run_*` database, and drop only that database afterward. The test role needs `CREATE DATABASE` permission; this avoids deleting or rewriting immutable historical events between test runs.

For an existing first-increment checkout, upgrade with `docker compose --env-file .env -f deploy/compose/compose.yaml -p ddia-go stop api worker`, then `make up`. This preserves the database and applies the new migrations before starting updated binaries. Existing holds receive a fifteen-minute grace period at the expiry migration. Older workers cannot consume the new lifecycle event types; this increment uses a coordinated local upgrade.

## Development boundary

This increment is for local development and learning. The two static tenant keys are a bootstrap authentication mechanism, not OIDC. Database connections in the Compose profile use a dedicated local network without TLS, and the scratch image has no general-purpose CA bundle. Production identity, tenant RLS, remote TLS trust, infrastructure, operational hardening, and full recovery certification remain planned. No real payment account or cloud account is required.

The supplied source currently has no open-source license grant; license selection remains a repository-owner decision.
