# Column-store publication and recovery

Use `make verify-columnstore` for the isolated warehouse scenario. It owns its temporary data and compiler directories and removes them after generating the JSON report. Reproduce a failed test/report before changing an existing database.

The durable store is a bbolt file containing metadata, version heads, a row delta, immutable segment blobs and cube cells. bbolt provides transaction recovery and the single-writer file lock. Reopening validates complete segment checksums/structure/zone statistics, current head locations, delta coverage and the cube recomputed from live facts.

An error at a `*.ready` failpoint aborts the transaction and leaves the previous publication readable. The native suite kills the process at `ingest`, `flush`, `compact` and `cube` ready/committed boundaries. These process kills retain the OS cache and are evidence of atomic publication at those boundaries, not power-loss certification.

An I/O commit error or `*.committed` failpoint error returns `ErrRecovery` and poisons the handle. Close and reopen the same file. The last requested operation may have committed despite its error; check its current fact/version or logical state before submitting a newer version. Earlier acknowledged synchronous operations must remain part of the recovered state under bbolt's filesystem contract. Exact duplicate versions are rejected by this lab's ingestion API rather than treated as duplicate command receipts.

Do not remove version heads to make a stale update pass. Deleted heads deliberately survive full compaction. Do not edit zone maps to avoid a checksum failure, adopt an unknown file, clear cubes after a failed open, or infer an active segment set from keys outside the stored metadata. A validation failure is an error, not permission to substitute an older snapshot.

`RebuildCube` recomputes cached cells from current validated facts and atomically replaces them; it does not repair corrupt source columns or metadata. Full compaction removes superseded segment blobs and row-delta entries, but bbolt can retain freed pages, so database file length need not shrink. Keep a copy of a failing file for investigation before any repair tooling is introduced.

Compiled predicates are separate owned temporary executables. Parse through the bounded grammar, compile through the approved template, and reuse the matching program across queries. Close it when done. A missing Go compiler/build failure or query-context cancellation is an execution error and does not mutate stored facts.
