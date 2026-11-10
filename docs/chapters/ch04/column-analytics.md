# Chapter 4: Columns, query execution and materialized analytics

The [column-store guide](../../COLUMNSTORE.md) defines the implemented format, API and actual measurement boundaries. `make verify ID=C04-04` runs the following warehouse experiments. Named experiments do not replace the full book-section/index semantic audit.

| Section | Experiment or worked exercise |
|---|---|
| B04-020 — Data Storage for Analytics | Compare full synthetic shipment/adjustment facts with selective column projections and grouped reductions; distinguish these scans from authoritative OLTP commands |
| B04-021 — Cloud Data Warehouses | Work through [storage/compute separation and publication](cloud-warehouse-design.md), with explicit mapping to this local implementation and the distributed pieces still absent |
| B04-022 — Column-Oriented Storage | Decode predicate/visibility columns and defer projected columns; compare requested frame bytes with a full-row representation baseline |
| B04-023 — Column compression | Round-trip dictionary-RLE, dictionary-bitmap, plain/RLE integer and validity encodings; compare actual encoded sizes |
| B04-024 — Sort order in column storage | Encode identical shuffled facts with/without tuple sorting, then inspect compressed sizes and row-group zone pruning |
| B04-025 — Writing to column-oriented storage | Query the durable row delta and immutable segments together; update/delete before flush, publish, compact, restart and retain tombstone floors |
| B04-026 — Query Execution: Compilation and Vectorization | Match scalar AST evaluation, native generated Go and mask-based batch evaluation, including SQL unknown values; report build/process/protocol costs and avoid a SIMD claim |
| B04-027 — Materialized Views and Data Cubes | Retract/add eight tenant-preserving grouping sets, keep nonnull counts, rebuild cached cells and compare every grouping with full reduction |

## Order and projection have different effects

Sorting clusters tenant/warehouse/day values and can improve both compression and group exclusion. Zone maps conservatively determine whether a group might match; they do not establish that every row does. The engine checks current visibility and the actual predicate afterward. `NOT` uses conservative pruning instead of risking an unsound inversion in the presence of nulls.

Projection avoids decoding unneeded column frames even within a group that must be examined. Visibility still needs identity/version information, and the delta still reads full JSON records. The report records those costs separately. Its frame-byte counts do not describe physical reads through bbolt's mmap or a cloud object reader.

## Updates cannot be filtered independently

Suppose version 1 contains ten units in the east warehouse, and version 2 moves the fact west with a null units value. Reading the old east group and filtering before reconciling versions could return a fact that is no longer east. The exact head location excludes it first. A delete keeps a version floor through compaction, so an older delivery cannot restore it.

One bbolt transaction publishes the fact delta, head and materialized changes. Flush/compaction use the same atomic publication boundary for new segment blobs and their heads. Precommit faults roll back; a postcommit failure has an unknown caller outcome and requires reopening. The lab tests actual process kills at both boundaries without claiming to reimplement bbolt's internal recovery.

## Aggregates need null and grouping identity

Three rows containing null, zero and five units have row count three, nonnull count two, sum five and average 2.5. A null category forms an actual group. A rolled-up category means all categories, and an empty string is another actual value. The key's grouping bits preserve all three meanings.

The cached cube answers its unfiltered grouping sets. Filtering by arbitrary row measures requires scanning/reducing the matching facts, rather than reusing incompatible cached totals. Both paths expose the same logical watermark, and a complete cube rebuild must reproduce it.
