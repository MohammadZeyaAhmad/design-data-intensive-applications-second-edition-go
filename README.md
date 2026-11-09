# Atlas — Designing Data-Intensive Applications in Go

A practical companion to **Designing Data-Intensive Applications, second edition** by Martin Kleppmann and Chris Riccomini. The goal is one cohesive Go backend and a complete set of connected data-system experiments, with equal emphasis on learning and production engineering.

**Implementation has started.** This first increment implements transactional inventory and durable projections. It does **not** yet implement the full book. The [complete plan](docs/plan/ddia-go-mega-project-plan.md), [88-package backlog](docs/plan/implementation-backlog.md), and [current status](docs/STATUS.md) distinguish the eventual scope from working code.

## Run the working implementation

Prerequisites: Go 1.27+, Docker with Compose, and GNU Make. The verified toolchain is Go 1.27.3. Default ports are **18081** for HTTP and **15433** for PostgreSQL; both bind to loopback. Commands run from the repository root.

```sh
make doctor
make up
make demo
make verify
make down
```

`make up` generates random local credentials in ignored `.env` (mode 0600), compiles a static Go binary, builds its container image, starts pinned PostgreSQL, runs checksum-validated migrations, and starts the API and projection worker. Existing `.env` is preserved. `make down` retains database volumes.

`make demo` receives ten units, reserves two, and commits that reservation using the real API. `make verify` runs domain/HTTP/architecture checks, Go race detection, real PostgreSQL integration tests, the lost-update lab, source-coverage checks, live HTTP contention, and worker/API restart recovery. Recovery verification stops/restarts only this project's local `ddia-go` services; run it on a development stack. Generated reports are in ignored `artifacts/`.

## Implemented behavior

- Inventory aggregate with explicit available/reserved/allocated invariants and bounded quantities.
- Vertical slices for stock receipts, reservations, commitment, and release.
- Tenant identity bound to a development bearer credential; callers cannot select another tenant with a header or body field.
- PostgreSQL row locking and optimistic version checks, with composite tenant keys and database constraints.
- Transactional request idempotency: concurrent duplicate requests return one saved outcome; changed payloads conflict.
- Transactional outbox and a separate worker that maintains an asynchronous inventory read model.
- Per-SKU event ordering, inbox deduplication, bounded retries, and quarantine that blocks only the affected stream.
- Checksum-protected, transactionally locked migrations; graceful process shutdown and readiness checks.
- Independent Go lab showing a lost-update schedule, connected to the real database contention tests.

Inventory state is authoritative in PostgreSQL tables. The outbox is not an event-sourced order store, and the local dispatcher is not Kafka. Ordering event sourcing, checkout, payments, reservation expiry, Kafka/CDC, additional domain contexts, full telemetry, and the remaining laboratories are next milestones.

## Repository map

```text
cmd/atlas/                         API, worker, migration, and schema-check commands
cmd/guide/                         Local configuration, demos, verification, source inventory
internal/inventory/domain/         Pure aggregate and state-machine rules
internal/inventory/features/       Use cases grouped by feature
internal/inventory/ports/          Tenant-scoped transaction and query interfaces
internal/inventory/adapters/       PostgreSQL and HTTP implementations
internal/platform/                Configuration and identifier utilities
migrations/                       Embedded, checksum-validated SQL
labs/                             Separate Go module for educational implementations
tests/architecture/                Enforced domain/feature import boundaries
tests/integration/                 Real PostgreSQL concurrency and failure tests
deploy/compose/                    Reproducible local services
docs/                             Guide, decisions, status, and full-book plan
```

Start with the [API contract](docs/API.md), [architecture decision](docs/adr/0001-inventory-walking-skeleton.md), and [transaction walkthrough](docs/chapters/ch08/inventory-transactions.md). See [validation](docs/VALIDATION.md) for measured evidence and limitations.

## Full-book scope remains mandatory

The planning inventory covers **14 chapters, 476 substantive headings, 61 glossary terms, 4,215 index occurrences, and 48 backend-pattern entries**. Every concept requires an implementation, executable experiment/model, or appropriate worked design exercise. Runtime profiles limit resources, not final scope.

```sh
make coverage-check     # validates the saved source inventory, not completeness of code
make lab                # runs the first deterministic lost-update demonstration
make integration        # fails if the local atlas_test binding is missing
```

The original plan is preserved under `docs/plan/`; [docs/STATUS.md](docs/STATUS.md) is the current implementation record. The EPUB and extracted book prose are intentionally not part of this repository.

## Development boundary

This increment is for local development and learning. The two static tenant keys are a bootstrap authentication mechanism, not OIDC. Database connections in the Compose profile use a dedicated local network without TLS, and the scratch image has no general-purpose CA bundle. Production identity, tenant RLS, remote TLS trust, infrastructure, operational hardening, and full recovery certification remain planned. No real payment account or cloud account is required.

The supplied source currently has no open-source license grant; license selection remains a repository-owner decision.
