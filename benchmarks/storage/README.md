# Storage experiment protocol

Run from the repository root with Go 1.27+ and the local PostgreSQL test binding:

```sh
make verify-indexes
```

The [index guide](../../docs/INDEXES.md) defines each ratio and the transaction/tuple layouts. JSON reports and the Go benchmark output are ignored under `artifacts/`; CI retains them in the `storage-comparison` artifact.

For a larger controlled run on a selected filesystem:

```sh
cd labs
go run ./cmd/lab tradeoffs -rows 4096 -runs 5 -workdir /your/experiment/directory -output ../artifacts/tradeoffs-large.json
go run ./cmd/lab indexes -rows 4096 -runs 5 -workdir /your/experiment/directory -output ../artifacts/indexes-large.json
```

Both runners create and remove only owned temporary children. They never delete/adopt existing databases. Run one experiment at a time, record the report's filesystem/Go/runtime/configuration metadata, preserve all raw runs, and report concurrent workload and container limits. Physical device information is explicitly unavailable when the runtime cannot identify it; record independently verified device/model/mount details beside results before making device claims.

The controlled storage defaults are 512 initial keys, synchronous mutations, three fresh-database repetitions and both sequential/random byte-key order. Final logical contents must match across every run. LSM memtable/block/table/fanout settings and B+ tree page/order limits are in each result. bbolt uses one synchronous transaction per mutation. Auto compaction and the final LSM flush are included in total write time; full compaction and reopening/validation are separate phases. Go method timing excludes fixture generation and reference comparisons. Percentiles are mutation latencies, not final-flush latency.

`make storage-bench` runs a complementary `go test -bench` point-read loop with repeated samples, allocation counts and a warmed 512-row fixture. Its selected parent is `ATLAS_BENCH_WORKDIR`; the Make target uses `artifacts/`. It includes the returned-value equality check in its timed loop. A short CI microbenchmark is a regression aid, not a statistically powered hardware study.

There are **no claimed cold-OS-cache measurements**. Reopened handles retain the OS cache, and `Open` validates pages/tables, warming data. This increment does not change global cache state. Host-controlled cache/device trials with a documented isolation procedure, real device counters and larger datasets remain outstanding; retain this gap when interpreting the report.

The PostgreSQL command accepts `-rows 4096..131072`, `-runs 1..5` and `-output`. It creates only a dedicated connection's temporary fixture. It captures server/planner/cache/fsync settings, all raw JSON plans, buffer counts, every sample and population variance. `VACUUM (ANALYZE)` precedes each warmed case. PostgreSQL temp relations use local buffers; their costs are not the application's regular shared-buffer workload. No `enable_*` switches or global database configuration are changed.

Avoid assertions that one engine must be faster. Assertions concern query correctness, plan structure, counter boundaries and valid denominators. The custom B+ tree's low fanout, full-page redo and extra separator reads are learning choices; container filesystem results do not generalize to production SSDs.
