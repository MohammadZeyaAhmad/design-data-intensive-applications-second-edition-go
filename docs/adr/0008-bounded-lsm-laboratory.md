# ADR 0008: A bounded storage engine in the independent lab module

Status: accepted for the eighth implementation increment.

Implement the storage mechanics in `labs/ch04/lsm`, using synthetic warehouse snapshots to connect them to the application domain. Retain PostgreSQL authority and the import boundary that prevents production packages from importing labs. Root guide commands invoke the lab executable through a subprocess and need no service credentials.

Use checksummed WAL records, a hash-map memtable, immutable sorted runs, sparse indexes and Bloom filters. Persist a versioned active-file manifest. Sync immutable files and their directory names before publishing that manifest; commit its rename before removing inputs or resetting the covered WAL. Reconcile by sequence rather than filename. Keep deletes in partial compactions and drop them only with full active-set coverage.

Provide size-tiered and leveled policies, individual synchronous/asynchronous key operations and explicit resource/format limits. Serialize operations with an in-process mutex and exclude other writers with an advisory lock. Uncertain mutation failures poison the handle until reopen. Corrupt complete records fail closed; an incomplete final append can be truncated.

Test the storage protocol against an independent in-memory map, a filesystem model separating data/name durability, and real child-process kills. Compare logical outcomes and raw timings with bbolt 1.4.3 through an adapter with synchronous transactions. bbolt is an implemented B+ tree baseline; this does not complete the separate custom B+ tree package.

The reference is intentionally bounded and synchronous. Background scheduling, compression, wider format-compatibility/scale stress and production storage certification remain further work. Raw warm-cache timings are evidence for this workload, without establishing hardware-wide performance rankings. C04-01 stays in progress until its complete book sub-concept audit and remaining work are reviewed.
