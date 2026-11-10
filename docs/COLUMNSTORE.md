# Column storage and warehouse analytics

This lab stores synthetic warehouse shipment/adjustment facts in a custom compressed column format. It implements immutable sorted segments, row-group pruning, a durable update/delete delta, three predicate execution modes, filtered aggregates and an incrementally maintained data cube. The application's authoritative contexts remain in PostgreSQL.

```sh
make verify-columnstore
make verify ID=C04-04
go run ./cmd/guide verify-columnstore
```

Run from the repository root with Go 1.27+ on the verified Linux filesystem contract. These commands require no application services or credentials. The generated-predicate mode needs the Go compiler on `PATH`; it builds a standalone standard-library-only program with module/workspace downloads disabled. Commands run race-enabled tests and produce `artifacts/columnstore.json`. CI retains that report, including generated Go source, source hashes and raw measurements.

```sh
cd labs
go run ./cmd/lab columnstore -rows 2048 -runs 3 -output ../artifacts/columnstore-large.json
```

`-workdir` selects a parent filesystem. The runner creates/removes only owned temporary children, including compiler output. It never adopts or resets an existing database.

## Facts, segments and visibility

Facts contain ID, version, tenant, warehouse, nullable category, day, nullable units and nullable net cents. The last two measures can be negative to model adjustments. Null remains distinct from zero; a null category remains distinct from an empty category. The store requires positive bounded IDs/versions, valid UTF-8 names and bounded measure/day values.

Each fact ID belongs to one tenant and accepts strictly increasing versions. `Put` and `Delete` persist a delta record, version head and cube changes in one synchronous bbolt transaction. The delta is row-oriented JSON. A tombstone's version floor survives flush and full compaction, preventing an old upsert from resurrecting a deleted fact. These are versioned ingestion primitives, without a production event-source/CDC adapter or duplicate-command receipt API.

Flush sorts live delta rows by `(tenant, warehouse, day, category, ID)`, builds immutable segments, moves their heads to exact segment/group/row positions and clears the delta atomically. Older segment versions remain until compaction. Full compaction rewrites only current live facts and removes superseded segments, while retaining deleted version heads. Reopening validates every segment and recomputes the expected cube from current facts.

**Visibility is resolved before filtering.** An old matching segment row is excluded when a newer version no longer matches, moves to another warehouse/day, becomes null or is deleted. A query also applies mandatory tenant equality outside the user predicate, so `OR` and `NOT` cannot widen its tenant scope. This is a local library boundary; it does not replace application authentication/authorization.

The engine serializes writers and holds a stable read transaction and read lock for a whole query. bbolt supplies atomic page publication and recovery. The custom segment is a checksummed blob inside bbolt; this increment does not implement a new page/WAL engine or an object-storage service.

## Column format

| Component | Representation |
|---|---|
| Segment | `ATCOL001` header with bounded lengths, row/group counts, metadata CRC32C and header CRC32C |
| Groups | Up to 64 rows, per-column offset/length/codec and min/max/null-count zone maps |
| Strings | Sorted dictionary plus either run-length codes or per-value membership bitmaps |
| Integers | Plain little-endian int64 or run-length values, whichever is smaller |
| Nulls | Validity bitmap; canonical null payloads remain distinct from actual zero/empty values |
| Column frame | Codec/type/count/validity/length header, encoded values and CRC32C |

Auto encoding chooses the smaller string representation for each group. Explicit dictionary-RLE and dictionary-bitmap variants support comparison. The report also encodes the same shuffled input without sorting to expose sort-order effects. Those diagnostic blobs pass the same codec validation; normal publication always sorts.

Header/offset/size checks bound parsing. Full validation reconstructs facts, checks ordering and uniqueness, and verifies zone maps against decoded columns. A checksum-valid false null count is rejected rather than trusted for pruning. Query execution checks selected frames and decodes only predicate, visibility and requested projection columns; projection columns are deferred until a group has selected rows.

Limits are 65,536 retained ID/version heads, 4,096 delta records, 64 segments, 4,096 rows per segment and 8 MiB per segment. Deleted IDs retain their version floors and still consume the head bound. The format is a bounded learning implementation, with broader scale/schema/format evolution still open.

## Predicate execution

The grammar allows schema-column comparisons with typed string/integer/NULL literals, `IS [NOT] NULL`, `AND`, `OR`, `NOT`, parentheses and Boolean constants. It has no calls, imports, SQL execution or arbitrary Go. Filters are bounded to 1 KiB, 256 tokens, 64 nodes and depth 16. String literals use SQL doubled quotes; generation uses Go-quoted literals and whitelisted schema names.

| Mode | Actual execution |
|---|---|
| Scalar | Interpret the typed expression tree for each visible row |
| Batch | Evaluate column vectors into uint64 true/unknown masks, using dictionary membership masks and bitwise Boolean operations |
| Compiled | Generate a direct Go predicate, build a native executable, send bounded column batches to its stdin and read selection masks |

All modes implement SQL three-valued logic. Only true selects a row; false and unknown do not. For example, `units = NULL` is unknown, and `NOT (units > 1 OR net_cents = NULL)` never selects a row. Batch execution uses Go loops and bit operations; it is **not an explicit SIMD implementation**. Compilation is native Go in a subprocess, with a bounded protocol; it is **not an in-process JIT**.

Parse a filter with `Parse`, build a reusable `Program` with `Compile`, and pass it in a `Query` with `Mode: Compiled`. Program identity must match the parsed expression. Close the program to remove its owned compiler directory. Query contexts cancel native execution; the store holds its read transaction until the query finishes. The generated source is retained in the report for review.

## Materialized aggregates and cube

Every accepted mutation retracts the old fact's contribution and adds the new fact to eight warehouse/category/day grouping sets. Tenant is always retained. Row count, measure sums and nonnull counts commit with the delta/head. `SQLSum` returns null when every value is null; `Average` uses nonnull count. Grouping bits distinguish `ALL` from an actual null category, an empty category or day zero.

`Materialized(tenant, grouping)` reads an unfiltered grouping at the current state watermark. `RebuildCube` replaces all cached cells from current facts in one transaction and must preserve that logical result. An arbitrary predicate cannot be answered from this unfiltered cache: `AggregateResult` instead reduces a filtered query that selected all required dimensions/measures. No average-of-averages shortcut is used.

## Evidence and measurement boundaries

The scenario compares independent full-row predicates/reductions with all three modes in sealed, delta, compacted and reopened phases. It verifies both tenants and all eight cached groupings in each phase. Tests add randomized mutations, null truth tables, projection/pruning, precommit rollback, unknown postcommit outcomes, stale-version rejection, codec corruption, concurrent access and eight native SIGKILL publication cases. Process kills retain the OS page cache; they do not certify physical power-loss behavior.

`compressed_column_bytes_decoded` counts selected encoded frames. Metadata and complete delta JSON bytes are separate. The visibility directory is also traversed and counted. bbolt mmap, filesystem allocation and device I/O are **not** measured by those counters. Segment bytes exclude the heads, cube and bbolt free-page overhead; total database file bytes are reported separately and need not shrink after compaction.

Wall time and Linux process user/system CPU deltas describe complete queries, including runtime work. Native helper CPU is separate. Compilation is a separate cost; native query time includes process startup and JSON serialization. The row oracle already has materialized rows and reports logical row-JSON bytes, without matching row decode costs. Raw repetitions describe warm process/OS state and establish no universal speed ranking.

See the [chapter experiments](chapters/ch04/column-analytics.md), [cloud-warehouse design exercise](chapters/ch04/cloud-warehouse-design.md), [publication runbook](runbooks/columnstore-recovery.md) and [decision](adr/0011-column-segments-delta-and-cube.md). C04-04 remains in progress for its complete semantic audit, distributed/cloud operation, production ingestion and broader format/scale/CPU work.
