# Rolling schema evolution and resumable backfills

C05-03 evolves the real Catalog brand representation from `name` to `display_name`. It keeps the public HTTP contract stable, runs actual historical/current Atlas executables together, resumes a crashed backfill, rolls back the binary, then contracts only after explicit retirement. Run from the repository root with the local PostgreSQL test binding:

```sh
make verify ID=C05-03
make evolution-demo
go run ./cmd/guide verify-evolution
```

The drill requires Go, Git, Make and a local `atlas_test` PostgreSQL database with permission to create isolated test databases. It creates a fresh `atlas_evolution_*` database, starts only its own API/worker subprocesses on loopback, retains its report/archive and removes that database afterward. It never contracts the development database. CI runs the same scenario against its disposable PostgreSQL service.

## Four stages and two actual binaries

The historical binary is built from immutable commit `393f703d7e097f96f09847b8196ead2c4429f260`; the current binary is built from the working checkout. The binary manifest records their source revisions, dirty state, Go version and actual SHA-256 digests. A shallow checkout must fetch that historical revision first; CI checks out full history. These are complete Atlas API executables, with different SQL/history implementations.

| Stage | Database behavior | Executable evidence |
|---|---|---|
| Expand | Add nullable display_name, a compatibility trigger, a stable read view and rollout metadata. Old tuples remain unfilled. | The old API continues serving while the new migration runs; both binaries read the same old values. |
| Backfill | Select bounded pending rows with `FOR UPDATE SKIP LOCKED`, fill metadata and commit a receipt/progress atomically. | A locked lowest key remains pending while later keys complete. A killed worker rolls back both data and receipt; another process resumes it. |
| Validate | Commit `CHECK ... NOT VALID` separately, then validate it under a DML-compatible lock. | Pending rows prevent validation. The completed non-null check is real PostgreSQL constraint validation. |
| Contract | Retire known old readers and rollback support, replace the read view, set NOT NULL and remove the old column in one transaction. | Early attempts fail; the old binary is stopped and retired, the real window expires, and only then does the current binary operate without name. |

Production startup applies only [the expand migration](../migrations/catalog/004_brand_display.sql). It never starts a backfill, declares rollback retired or drops a column automatically. The migration's source-controlled bounded-lock marker limits lock acquisition to 250 ms and restores the caller's prior setting. Historical migration bytes/checksums remain unchanged.

During coexistence, old writers update name and new writers update display_name. A database bridge synchronizes the other representation atomically and rejects inconsistent dual updates. New reads use an owning-context view that falls back to old data before backfill. During contraction the view changes to use only display_name, retaining its published SQL shape and generated sqlc mapping. Product details, REST and GraphQL continue to expose the same brand value. Brand business revisions and command response contracts are independent of the representation backfill.

## Resumable bounded work

The operator executable is `go run ./cmd/schema`; supply `ATLAS_SCHEMA_DATABASE_URL` explicitly. It accepts single batches or bounded resumable runs:

```sh
go run ./cmd/schema prepare
go run ./cmd/schema run -id brand-rollout-2026 -limit 32 -interval 100ms -deadline 5m
go run ./cmd/schema status
```

A repeated run ID replays committed batch receipts before continuing. Each receipt binds its batch ID, row limit and interval; changed arguments cannot reuse the same receipt. Interrupted/uncertain commits are resolved by that durable identity. Runs stop at their deadline or maximum batch count and expose whether they completed. Signal cancellation drains/rolls back the current database operation; a later invocation resumes from committed facts.

The progress predicate is always `display_name IS NULL`, with stable tenant/ID ordering. There is deliberately no saved cursor that advances past skipped locks. A zero-row batch is incomplete when the predicate still exists. Completion follows actual remaining data, rather than a processed counter: an ordinary writer can also fill an old row through the bridge. Backfill does not increment aggregate revisions or append fictitious business events.

Index preparation executes real `CREATE INDEX CONCURRENTLY` outside a transaction. It checks validity and definition, repairs an interrupted owned build, and refuses an unexpected object under the reserved name. The partial index supports selecting remaining rows; per-batch completion uses `EXISTS` rather than recounting an ever-shrinking table each time. Backfill SQL/idle-transaction timeouts are 300 ms, lock acquisition 100 ms and the transaction context one second. Throttling uses a persisted database timestamp and an advisory lock serializes job progress/receipts, while ordinary commands continue using their existing row/version locks.

The drill kills a real schema worker while an owned database trigger pauses its receipt insert. It verifies both data and receipt rollback, then discards a committed reply and resolves its receipt from another process. Actual old/current HTTP writers overlap resumed backfill work. The report retains each foreground request duration/revision, each observed batch and process wall time. HTTP probes have a two-second deadline; these are observed fixture latencies and enforced local budgets, without a production latency guarantee.

## Rollback and retirement

The new API is stopped and the historical executable is launched again after new-format writes. It reads/writes successfully because the bridge and old column remain. No down migration is used for binary rollback.

Validation is split into two transactions so the initial metadata lock is released before the table scan. Combining both operations in one transaction would retain ACCESS EXCLUSIVE during validation. Contraction obtains a 250 ms lock-acquisition budget and rolls back atomically if a reader holds the table. Failed attempts preserve the prior rollout phase.

The operator must open a rollback window; its CLI default is seven days, and an existing deadline can only be extended. Closing the window requires actual database time, explicit retirement of every declared legacy reader, and no observed `atlas-legacy/` sessions in the target database. Lease expiry alone does not retire a reader. Retirement and contraction have immutable audit entries with operator/reason. A retired legacy capability cannot renew its registration.

The isolated demonstration uses a short real window. Deployment inventory and session labels are trusted operator assertions. They do not identify arbitrary unregistered readers or replace version-specific database roles, deployment fencing, replica/warehouse inventories or production approval policy. Those remain operational work; there is no claim that a timer alone makes destructive DDL safe.

## Independent dataflow contracts and archives

| Dataflow | Contract |
|---|---|
| Public brand HTTP | id/name/version stays stable across both binaries. |
| Current database | Physical name/display_name columns coexist, then the stable owning-context view survives contraction. |
| Immutable brand history | An explicit schema_version column distinguishes v1 name from v2 display_name payloads. Aggregate version is a separate field. |
| Archive | Format 1 retains raw JSONB source images, metadata, timestamps, body/metadata digests and both exact writer schema definitions. |

Old binaries continue writing history schema 1 via its database default; the new owner explicitly writes schema 2 through a published wire type. Upcasters choose by the stored schema version and transform in memory. They reject unknown/future schemas, invalid identities, duplicate keys and invalid types. Original source/history rows remain immutable; their stored JSONB text digests stay unchanged through backfill, rollback, contraction and replay. JSONB source images are PostgreSQL's normalized representation, without a claim to preserve original HTTP whitespace.

The operator exports one tenant/brand's complete bounded history to an immutable local artifact and replays it offline:

```sh
go run ./cmd/schema export -tenant acme -brand maker -file artifacts/brand-history.json
go run ./cmd/schema replay -file artifacts/brand-history.json
```

Replay needs no database environment. It verifies writer definitions, source hashes and every record before returning a complete canonical answer. Both old/new history formats remain readable after their original physical column is gone. Publication uses synced temporary files and atomic create-if-absent linking; an identical byte retry succeeds and different content cannot overwrite the same path. Exports cap at 256 records/1 MiB and fail explicitly when that bound is exceeded. This is a private local archive of the selected source stream, without a complete database backup, signature, physical power-loss or object-store WORM claim.

The report is `artifacts/evolution.json`, with a retained `evolution-run-*` archive and `evolution-binaries/manifest.json`. C05-03 remains in progress for its full semantic audit, larger-scale/multi-client operation, stronger reader fencing, broader transforms and retention/restore governance. The next planned package is C05-04 REST/RPC, discovery and transport failures.
