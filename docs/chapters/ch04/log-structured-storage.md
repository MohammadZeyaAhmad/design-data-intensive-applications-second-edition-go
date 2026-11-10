# Chapter 4: From an append log to immutable sorted runs

Package C04-01 connects storage mechanics to the project's warehouse stock read models. The lab uses synthetic snapshots with keys such as `tenant/acme/warehouse/east/sku/000002`. They are ordinary bytes to the engine; prefix layout is not an authorization boundary. Five stores must return the same current snapshot after updates, deletes, compaction and reopening.

Run `make verify-storage`, then inspect `artifacts/storage.json`. The [storage guide](../../STORAGE.md) documents the format, durability contract, limits and API. The source starts at `labs/ch04/lsm/db.go`.

## Named concept scenarios

| Book section | Scenario and implementation |
|---|---|
| B04-001 — OLTP storage and indexing | Compare point updates, point reads and ordered tenant scans over the warehouse snapshot dataset; distinguish this key/value API from the application's transactional aggregate writes |
| B04-002 — Log-structured storage | The append-log configuration writes checksummed frames and reconstructs its memory index on reopen; overwrites append rather than editing earlier frames |
| B04-003 — SSTable format | Flush sorted records; use per-frame CRCs, sparse block indexes, Bloom metadata, a bounded footer and a manifest-recorded file hash |
| B04-004 — Construction and merging | Sort the memtable at flush; merge sorted run cursors with a heap, reconcile duplicate keys by sequence and stream bounded output tables |
| B04-005 — Bloom filters | Check every inserted key for false negatives, measure misses on a fixed absent-key fixture, and verify that filtered deletes cannot expose older values |
| B04-006 — Compaction strategies | Run the same workload under size-tiered and leveled policies; retain tombstones in partial merges and prove a full compaction can remove them safely |
| B04-007 — Embedded engines | Compare the Go implementation with bbolt's synchronous copy-on-write B+ tree through the same point/range API, including empty values and copied read results |

## Why record sequence differs from file age

Suppose file 1 contains `BOOK=old` at sequence 10. File 2 contains its delete at sequence 20. A partial compaction can rewrite file 1 as file 3 while leaving file 2 untouched. Looking at the newest filename would expose the old value. The engine instead reconciles by record sequence, so the delete still wins. The regression test also updates the key again, rewrites the old run, and reopens the database.

A tombstone can disappear only when the compactor knows it has included all older versions. Automatic partial compactions conservatively keep it. Explicit full compaction includes the entire active set, while holding the single-writer mutex, and can drop both the marker and its obsolete values. Old physical files that survive a crash remain outside the manifest and cannot resurrect the key.

## The two commit boundaries

There are separate fences for a successful write and a flush. Write durability comes from the WAL fsync. Flush durability comes from table-file/name persistence followed by manifest persistence. The WAL cannot be reset merely because a table file exists. Likewise, compaction cannot remove inputs merely because a new manifest file was written; the manifest rename must be durable first.

`crashfs_test.go` drives the shared `labs/internal/diskmodel` model of file contents and directory names independently. It explores crashes after each filesystem mutation under buffered writes, early data persistence, early manifest-rename persistence, early deletions and short writes. Each recovered sequence must be a submitted prefix at least as long as the acknowledged/fenced prefix, and its complete map must match that exact prefix. The oracle does not merely check that the database opens.

`process_test.go` separately kills real child processes at 21 named boundaries and checks file-lock exclusion, the acknowledged prefix and the recovered map. Those are process failures on the actual filesystem. The model supplies the power-loss schedules; neither check certifies a physical device or a filesystem implementation.

## Trade-offs to inspect

The hash-map memtable gives direct point lookup and sorts at flush/scan time. One mutex makes recovery and publication easy to audit but allows flush/compaction stalls. Leveled files limit overlap above L0; tiered runs can reduce rewriting while retaining more overlapping runs. This implementation compares sequences across candidate tables rather than assuming the first file must win. Sparse indexes seek both point reads and range starts; Bloom filters help point misses, not arbitrary prefix scans.

Startup verifies complete active-file hashes. That catches missing/damaged immutable files eagerly and adds startup I/O. A production engine often validates individual blocks lazily and has richer file caches, compression, background scheduling, snapshots and recovery tooling. These choices and the workload's raw measurements are explicit starting points for the remaining storage packages.
