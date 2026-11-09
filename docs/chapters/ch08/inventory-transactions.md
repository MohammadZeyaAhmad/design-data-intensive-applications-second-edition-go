# Chapter 8: Transactions through inventory reservations

Related work packages: C08-01, C08-03, C08-08, APP-03, APP-06. This page covers the implemented subset; the full chapter remains in progress.

Two clients can both read stock before either writes it. If each computes the next value from that old snapshot, one write can overwrite the other. The first Go lab schedules this explicitly, without relying on a timing accident or introducing a Go memory data race:

```sh
make lab
```

Predict the result of two increments. The unsafe read/modify/write schedule finishes at 1; indivisible increments finish at 2. The lab asserts that the counterexample occurred, so a correct demonstration exits successfully.

The application turns this into a business invariant: ten available units must admit at most ten simultaneous one-unit reservations.

```sh
make up
make verify-live
```

The live scenario submits 100 HTTP requests for ten units. It expects exactly ten successful reservations and 90 insufficient-stock responses, then verifies the authoritative counters and asynchronous projection. A second tenant cannot read this stock.

Read the implementation in this order:

1. `internal/inventory/domain/stock.go`: validated quantities, available stock, invariant-preserving transitions.
2. `internal/inventory/features/reservestock/handler.go`: the complete use case inside one transaction boundary.
3. `internal/inventory/adapters/postgres/store.go`: unique request identity, row locks, expected-version saves, and rollback.
4. `migrations/inventory/001_inventory.sql`: tenant-scoped constraints and keys.
5. `internal/inventory/adapters/postgres/worker.go`: ordered delivery, inbox receipt, and a rebuildable-in-principle derived representation. An operator-facing full rebuild command is not yet implemented.

`make integration` also checks 40 simultaneous retries of the same command, changed-payload rejection, failure after the outbox append, commitment versus release, cross-tenant writes, locked stream ordering, two workers, duplicate delivery, quarantine/repair, and migration-checksum drift.

`make verify-recovery` stops the worker, accepts durable commands, verifies that the projection is still absent, then restarts the worker and checks catch-up. It restarts the API and repeats a successful command with the same key, requiring the same saved response. This is restart/outage evidence; it is not a claim that every possible crash instruction or ambiguous remote commit has been tested.

Trade-offs: the locked row serializes changes to a hot SKU; the database is a single local instance; active reservations have no automatic expiry yet; idempotency records currently retain indefinitely. Later labs will compare conditional updates, serializable transactions, sharding/escrow, and broker-based delivery using these same invariants.
