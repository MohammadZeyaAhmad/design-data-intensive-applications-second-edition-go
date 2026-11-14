# Single-leader replication and recovery

This C06-01 scenario uses the actual Atlas Inventory HTTP feature and PostgreSQL 18 physical replication. The separate Go model makes acknowledgment, transport, replay and failure-domain assumptions visible. Neither component supplies automatic leader election.

Run from the repository root with Go 1.27.3, Docker/Compose and Make:

```sh
make replication-model replication-demo
# With the ordinary local test PostgreSQL configured/running:
make verify ID=C06-01
go run ./cmd/guide verify-replication
```

`replication-demo` builds the current Atlas executable and an independent lab executable. By default a digest-pinned PostgreSQL image runs the lab as an unprivileged user, with no external network, readonly application binaries and disposable tmpfs data. The lab owns multiple postmaster processes on loopback; they share one host and failure domain. They are independent database processes, without independent storage or availability zones. Fsync is enabled, but process crashes in tmpfs do not test persistence across host power loss. No default application database or existing volume is used by the native scenario.

To use an installed PostgreSQL 18 runtime instead, set `ATLAS_REPLICATION_PG_BIN` to its absolute binary directory. `ATLAS_REPLICATION_PG_SHARE` optionally selects shared files for `initdb`; the runtime must retain a valid installation layout and its native libraries. The runner refuses root, allocates private temporary directories, generates disposable credentials, bounds runtime to five minutes and terminates only its own processes. `ATLAS_REPLICATION_WORKDIR` selects the temporary-data parent. Allow approximately 1 GiB for the sequential fixtures. Native mode and Compose execute the same Go runner.

Reports are written to `artifacts/replication/model.json` and `artifacts/replication/native.json`. `artifacts/replication-binaries/manifest.json` identifies the Git revision, dirty state, Go version and SHA256 of both binaries. CI retains these artifacts. Reports contain positions and evidence, without credentials or connection strings.

## Acknowledgment is a durability contract

| Mode | Controlled fault | Expected acknowledgment and failover result |
| --- | --- | --- |
| Asynchronous, `synchronous_commit=on` without a synchronous standby | Disconnect WAL transport, commit a real Inventory receipt, fence the primary and promote the stale follower | Local commit receives HTTP 200; the promoted follower loses the one acknowledged probe |
| Synchronous `on` with `FIRST 1 (atlas_replica)` | Pause replay while receiving/flushing WAL continues | HTTP 200 precedes read visibility; the LSN read gate times out; resuming replay and promoting after primary fencing preserves the probe |
| Synchronous `remote_apply` | Pause replay while a real request waits in `pg_stat_activity` with `wait_event=SyncRep` | Reply is withheld until replay resumes; a successful reply is visible on that follower and survives promotion |

The report measures primary flush, follower receive and replay positions, byte lag, local reply latency and acknowledged loss for each mode. Transport disconnection demonstrates actual missing WAL. Replay pause demonstrates visibility lag while durable WAL remains present. These are different faults. Timings are local fixture measurements, with no WAN latency or production RPO guarantee inferred.

A separate synchronous test disconnects the standby and cancels a request during the actual commit wait. The receipt is committed locally despite the missing acknowledgment. Reconnecting and retrying the same intent produces one receipt/effect. A timeout is an uncertain outcome; it does not establish rollback. The model and real scenario preserve this distinction.

## Promotion, fencing and rejoin

Bootstrap uses a real consistent `pg_basebackup` plus streamed WAL and a physical replication slot. A stopped follower catches up after writes continue. After failover, `pg_rewind` discards the old primary's divergent branch, and the old node rejoins read-only with a new physical slot. The lost asynchronous write is not silently merged.

The controller kills the owned old primary and its process group, waits for process exit, and confirms the endpoint cannot accept another connection before promotion. A still-running old Atlas API also fails its write. This process fence is the exclusive-writer precondition for this one-host exercise. A copied table or an incremented epoch cannot stop a partitioned old database from accepting writes. Production needs a separately established election/fencing authority and a verified fence before promotion.

The new immutable migrations add `platform.writer_fence` and statement triggers on all seven Inventory effect, receipt, workflow and projection tables. The fence defaults to disabled. When enabled, a trigger checks `ATLAS_DATABASE_WRITER_EPOCH`, supplied by trusted server configuration on every pgx connection. Its shared row lock lasts until transaction completion, so advancing the epoch waits for already admitted writers. The promoted database rejects old-token Inventory writes without partial effects; the new token succeeds.

Only the migration/control role may update the authority. The fixture application role has Inventory DML and schema-check permissions, without ownership, trigger-disable or authority-update privileges. A narrowly scoped security-definer trigger uses a fixed search path. The token is an operational incarnation marker, not a secret or protection against malicious SQL clients who can choose their own custom GUC. Other bounded contexts are not yet epoch guarded. Enabling the optional guard requires configuring API and worker writers together; use a maintenance boundary for this rollout.

## Replication does not replace a backup

The online backup overlaps sixteen real Atlas writes. `pg_verifybackup` validates its SHA256 manifest; changing a byte in a backed-up Inventory relation must fail verification. The archive helper fsyncs complete WAL before atomic publication, accepts an identical retry and rejects conflicting replacement.

After the backup, the lab records a named restore point and an invariant snapshot. A later acknowledged receipt and a deliberate `DROP TABLE inventory.stock CASCADE` reach both primary and standby. The lab then restores a separate database from the verified base backup and archived WAL, with `recovery_target_name` and target promotion. Its stock, reservation/allocation bounds, version, command receipts and outbox events match the snapshot; the real Atlas read returns the recovered stock. The post-target acknowledgment is deliberately absent. The damaged live replicas are not used as the restore target.

The report records recovery-to-validated-API duration and the intentionally excluded acknowledgment. That local duration is evidence for this fixture, not a production RTO. Backup retention, encryption, access policy, offsite placement, archive monitoring, point-in-time scheduling, tablespaces and large restore drills remain OPS-03/P41 work.

See the [chapter mapping](chapters/ch06/single-leader.md), [decision](adr/0024-single-leader-authority-and-independent-recovery.md), [recovery runbook](runbooks/replication-recovery.md), [increment plan](replication-work-plan.md) and [recorded validation](VALIDATION.md). C06-01 remains in progress; C06-04 owns complete session consistency and application replica routing.
