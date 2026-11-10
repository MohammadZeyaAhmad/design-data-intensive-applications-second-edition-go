# Index layouts and controlled storage comparisons

The warehouse snapshot lab now compares heap rows, clustered rows, a composite secondary index, a covering index and an ordered in-memory index. It also measures sequential/random writes, read/write/space amplification in the existing stores, and real PostgreSQL plans. These are synthetic, isolated experiments; production contexts continue to use PostgreSQL.

```sh
make verify-indexes
make verify ID=C04-03
go run ./cmd/guide verify-indexes
```

The core checks need Go 1.27+ and a running local `atlas_test` PostgreSQL binding, normally provided by `make up`. The test role/URL comes from `ATLAS_TEST_DATABASE_URL` or the existing owner-only `.env`. The PostgreSQL command creates only session-local temporary objects and closes its dedicated connection afterward. It does not migrate, index or edit application tables.

| Command | Evidence |
|---|---|
| `make index-demo` | `artifacts/indexes.json`: five layouts, three repetitions and algorithmic work counters |
| `make tradeoffs-demo` | `artifacts/tradeoffs.json`: four stores × two write orders × three repetitions, raw and summarized measurements |
| `make query-plans` | `artifacts/query-plans.json`: heap, tenant-only, multicolumn and covering plans, three samples each |
| `make storage-bench` | `artifacts/storage-go-benchmark.txt`: repeated warm point-read Go microbenchmarks |

Make's scoped verification runs all four reports. The guide command runs the tests and three JSON reports; the Go microbenchmark is separately available through `make storage-bench`. CI retains these reports alongside the original storage/tree reports. Generated databases are owned temporary children, removed after the run. Use `-workdir` to select a filesystem without adopting any existing database.

## A query determines useful ordering

Every layout serves the same query: tenant and warehouse equality, a half-open available-stock interval, then ascending `(available, SKU, ID)` with a bounded limit. The projected fields are ID, SKU, available stock and version. Available stock is `on_hand - reserved - allocated`. Natural `(tenant, warehouse, SKU)` identity remains unique.

| Layout | Row location | Query work |
|---|---|---|
| Heap | Rows keyed by surrogate ID in bbolt | Decode/filter every row, sort qualifying projections, apply the limit |
| Clustered | Full row at `(tenant, warehouse, available, SKU, ID)` | Seek once and scan full rows in result order; an ID locator supports updates |
| Secondary | Heap rows plus ordered keys identifying rows | Seek and scan secondary entries, then fetch each matching primary row |
| Covering | Heap rows plus the same keys and included version | Produce the projection from index entries, without primary-row lookups |
| Memory | ID hash map and sorted composite-key slice | Binary-search the range and fetch from memory; insertion/deletion shifts are O(n) |

The durable layouts use one synchronous bbolt transaction for the primary row, natural-identity mapping, locator and secondary/covering changes. Rekeying removes old entries before installing new ones. Deletes remove corresponding entries. Conflicting natural identities and injected failures before commit roll the whole transaction back. The layout is persisted and checked on reopen. This demonstrates application-level index maintenance while bbolt supplies the page engine and transaction machinery.

String tuple components use zero escaping and terminators; unsigned integers use big-endian encoding. The representation preserves byte ordering and keeps names containing zero bytes or shared prefixes separate. Names are bounded valid UTF-8 strings. Rows have positive identifiers/versions, nonnegative stock and no over-allocation. The lab is bounded to 65,536 rows and 256 query results. The memory layout is volatile and cannot be compared with synchronous storage as if it supplied the same durability.

`Cost` counts full scans, seeks, examined index entries, decoded rows, primary/memory lookups and returned rows. It is not an I/O counter. A covering index avoids fetching unselected row columns, but adds payload to the index and increases maintenance. Clustered order is one physical key order: changing the query/order may require another index or sorting.

## Measuring amplification

The storage runner holds the logical mutation set, values, durability policy and read queries fixed. Inserts, every-fourth-row overwrites and every-seventh-row deletes are executed in sorted byte-key order or a recorded deterministic shuffle. Engine order rotates across repetitions. Fixture generation and oracle comparisons are excluded from method timing; the write phase includes inline automatic LSM compaction and the final memtable flush. Mutation percentiles exclude that final flush, which remains in the total write time.

| Metric | Numerator / denominator |
|---|---|
| Requested write amplification | Custom-engine WAL + table/manifest or DATA write bytes after initialization / submitted key and value bytes; deletion contributes its key |
| Requested read amplification | Bytes requested during the measured hit/range phase / returned key and value bytes |
| Miss cost | Requested bytes / missing-key queries; no invented zero-byte result denominator |
| File-space amplification | Current logical file lengths / live key and value bytes |

The report includes raw repetitions, min/max/median, mean, population standard deviation and coefficient of variation. Full LSM compaction is a separate timed/counted phase. Reopen validation is timed separately because opening the custom engines validates their files. bbolt reports time and file lengths; its unavailable byte-request counters are omitted rather than reported as zero.

These counts exclude filesystem allocation, journals, writeback merging and device-controller behavior. They are not measurements of physical SSD write amplification. The small eight-key B+ tree intentionally writes whole modified paths, so its large amplification is a property of this implementation and dataset. It is not a general B+ tree/LSM ranking.

## PostgreSQL plans

The PostgreSQL runner generates 32,768 wide fixture rows in `pg_temp`, then runs the same selective ordered projection before and after index changes. A tenant-only index is an intermediate comparison. The multicolumn index orders `(tenant, warehouse, available, SKU, ID)`; the covering variant includes `version`. The runner leaves `enable_seqscan`, `enable_indexscan`, `enable_indexonlyscan` and `enable_bitmapscan` unchanged.

After each DDL change it runs `VACUUM (ANALYZE)` and warms the query. It requires the actual optimizer to choose a multicolumn index scan and a covering index-only scan, with zero heap fetches in the vacuumed case. It preserves every `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plan, planner settings and timing sample. An ordering-key/included-version update and a delete must still match the independent fixture model. Index-only scans can need heap visibility checks after updates; the zero-fetch result is tied to the explicit vacuumed fixture.

See the [chapter experiments](chapters/ch04/index-layouts.md) and [benchmark protocol](../benchmarks/storage/README.md). C04-03 remains in progress: actual cold-OS-cache/device experiments, broader scale/concurrency and the complete section/index semantic audit remain outstanding.
