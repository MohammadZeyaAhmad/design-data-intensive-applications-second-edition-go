# Replication log formats

C06-03 compares three representations over warehouse stock and receipt fixtures: replaying SQL, PostgreSQL physical WAL, and actual `pgoutput` row changes consumed by Go. It uses a primary, a physical standby and an independently initialized logical projection. These are colocated processes, without independent availability zones or production CDC authority.

## Run

Use Go 1.27.3, Docker/Compose and Make from the repository root:

```sh
make verify ID=C06-03
go run ./cmd/guide verify-log-formats
make log-formats-demo
```

The default profile runs a digest-pinned PostgreSQL 18 image as the unprivileged `postgres` user, with readonly lab binaries, no external network and owned tmpfs data. Three independent postmasters and the consumer processes communicate on loopback inside that sandbox. The ordinary application database, test database and volumes are not used.

For native execution, set `ATLAS_LOG_PG_BIN` to an absolute PostgreSQL 18 binary directory and optionally `ATLAS_LOG_PG_SHARE` for `initdb` shared files. The installation must retain its valid library/share layout, and the runner refuses root. Native mode runs the same Go controller and allows four minutes. It stops only captured child processes and deletes their temporary directories. Allow about 1 GiB for the native fixture.

`artifacts/log-formats/native.json` records observations, completed consumer runs, crash boundaries, source identity and a matched state hash. The binary manifest records revision, dirty state, Go version and SHA256. Reports contain no credentials, raw tuple payloads or connection strings. CI retains the reports and binary provenance.

## What the representations preserve

| Representation | Native observation | Compatibility boundary |
| --- | --- | --- |
| Statement replay | Identical INSERT text produces different generated ID, clock value, current date and random sample under controlled session/sequence state | A statement protocol must specify or capture nondeterministic inputs and execution context |
| Physical WAL shipping | A real base backup and WAL receiver reproduce rows, schema expansion and sequence WAL; source and standby share cluster/catalog/block contracts | Compatible PostgreSQL physical format and a matching base image are required |
| Logical row changes | Go receives actual inserted/updated/deleted values, key images, NULLs, unchanged TOAST markers, TRUNCATE and transaction boundaries | Target schema/types/keys must be compatible; relation metadata is not an instruction to execute DDL |

The statement experiment deliberately varies timezone, random seed and sequence state. Source timezone UTC−12 and receiver UTC+14 guarantee different `current_date` values. This is controlled replay of the same SQL, without claiming PostgreSQL implements statement replication or that every statement protocol ignores such inputs. The logical consumer copies the resolved source values, so it preserves that receipt rather than re-running its functions.

The physical profile uses real PostgreSQL WAL shipping. It verifies matching cluster identifier, major, catalog version and block size. A unit preflight rejects mismatched contracts; only PostgreSQL 18 is executed natively. The logical projection has a different cluster identifier and explicit relational compatibility checks. Cross-major logical operation and alternate physical binaries remain separate audits.

## Snapshot and log handoff

The Go client creates a persistent `pgoutput` slot with an exported snapshot. It keeps the exporting replication connection idle while another SQL connection imports that snapshot into a read-only repeatable-read transaction. A separate source writer then commits stock, receipt and barrier changes. The snapshot copy still sees the earlier state, while the slot retains the overlapping transaction.

Bootstrap inserts the snapshot and its consistent-point checkpoint in one target transaction. Streaming starts from that position only afterward. The final row state must include the overlapping commit exactly once. Importing the snapshot after releasing its exporting connection, copying outside the imported transaction, or starting at an arbitrary current WAL position would break this handoff.

The target is a read projection over four allowlisted tables in `log_lab`. It is not the application Inventory writer. Bootstrap is scoped to this owned target; publication filters, partition routing, generated columns and arbitrary production schemas are outside this increment.

## Committed transactions and acknowledgments

The decoder buffers one bounded protocol-v1 transaction until COMMIT. The sink applies its row changes in wire order and commits them together with an ordinal audit, transaction receipt and checkpoint. A target lock serializes the checkpoint update. Stable source identity includes system identifier, database, slot, publication and a control-owned stream incarnation; XID alone is not a permanent identity.

Only the durable target **transaction-end LSN** is acknowledged. A keepalive's WAL end, a received frame position and the length of its payload are not durable checkpoints. A source position beyond the target checkpoint, a changed stream identity, a missing slot or lost WAL stops resume rather than silently jumping ahead. Recreating a slot/publication requires an appropriate incarnation/rebootstrap policy; the metadata token does not automatically fence arbitrary external administrator changes.

Actual SIGKILL drills stop the Go consumer immediately before target commit and immediately after target commit but before source acknowledgment. Before commit, row effects, audit and checkpoint all roll back. After commit, those effects survive while the source confirmed-flush position stays at the older checkpoint. A restart deliberately requests retained source data: the already committed transaction is deduplicated by its durable end position, then acknowledged. This demonstrates at-least-once transport with idempotent transactional projection, without claiming global exactly-once external effects.

The eight-operation fixture includes insert/update/delete of a transient row, an unchanged-key update, a key-changing update, TRUNCATE, another insert and a barrier. The final fixture has six unique applied source transactions, 21 audited changes, five receipt rows and BOOK quantity 24. Aborted source changes are absent; their sequence allocation leaves an observable gap. NULL, empty text and unchanged TOAST retain distinct meanings. An update with an incomplete image or no expected target row fails instead of inventing a replacement row.

## Schema and sequence operation

After source schema expansion, the source transaction first changes a compatible receipt and then touches the incompatible stock relation. The logical sink rejects the contract and rolls back the earlier receipt change too; its checkpoint remains unchanged. The physical standby already has the new column. Applying the explicit target migration lets the Go consumer replay and commit the complete transaction. Unknown types/columns, unsupported identity modes and binary tuple encoding fail closed.

Sequence increments are not ordinary row changes. After fencing the original source, two independent offline branch probes demonstrate the consequences. The logical target's unadvanced identity sequence first collides with an already copied row; an explicit sequence repair permits a new ID. The physical branch generates a noncolliding ID from replayed WAL state. Its value may jump because sequence WAL preallocates values; physical sequence state is not asserted to equal each source `last_value` observation exactly. These probes do not route concurrent application traffic to two writers.

A separate actual PostgreSQL publication probe refuses UPDATE on a table without a usable replica identity. Logical-slot failover, DDL orchestration, sequence promotion policy and production publication changes remain operator obligations.

## Bounds and evidence

Frames are limited to 1 MiB, buffered transactions to 4 MiB/256 changes, relation metadata to 16 relations/32 columns, and snapshot rows to 4,096 per table. Text, NULL and unchanged-TOAST cells are supported; binary values, custom type messages, streaming/two-phase transactions, logical messages and TRUNCATE restart/cascade options are not implemented. Consumer counters cover completed job reports; traffic from failed/killed jobs is not aggregated into those totals.

Wire preflight checks lengths and counts **before** the upstream parser allocates from them. Fuzzing found an oversized TRUNCATE-count allocation; its failing input is retained as a regression corpus, alongside explicit forged-length/count tests. Panic containment alone is not treated as an allocation bound. The fixed parser completed a finite 120,563-execution fuzz run without failure; this is evidence rather than a proof.

Keepalive feedback deliberately never fast-forwards the durable checkpoint across unprocessed work. Idle/unpublished-WAL progress, retention alarms, slot invalidation recovery, least-privilege production roles, initial-copy restart scheduling, cross-major compatibility and broader format/scale/operations audits remain open. Raw CDC rows describe storage changes; they do not automatically supply the business meaning of domain events.

See the [chapter mapping](chapters/ch06/replication-log-formats.md), [decision](adr/0026-logical-row-projection-and-durable-feedback.md), [runbook](runbooks/logical-replication-recovery.md), [increment plan](log-formats-work-plan.md), and [validation](VALIDATION.md). C06-03 remains in progress.
