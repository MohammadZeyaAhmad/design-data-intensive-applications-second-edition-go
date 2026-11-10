# ADR 0010: Atomic row/index layouts and explicit benchmark boundaries

Status: accepted, 10 November 2026.

The existing custom stores expose individual key operations. Maintaining a primary row and several secondary keys through that API would require additional transaction/recovery machinery. A crash between those writes could leave a stale index, which would undermine the comparison.

Use synchronous bbolt transactions for the durable index-layout experiments. A row, its natural warehouse/SKU identity, clustered locator and secondary/covering changes commit together. A bounded failpoint immediately before commit tests rollback after all structures have changed. Keep heap, clustered, secondary and covering layouts in separate owned databases with persisted layout identity. Use a separate volatile map/ordered-slice layout for in-memory behavior.

This choice makes index maintenance and query work the experiment. It does not claim a new transaction engine, PostgreSQL heap implementation or durable memory database. The independent LSM/B+ tree engines remain the storage-amplification comparison; their formats and durability protocols are unchanged.

Measure custom-engine byte requests with snapshots around explicit phases. Separate initialization, measured mutations/flush, queries, reopen validation and full compaction. Publish raw repetitions and variance, including the unavailable fields and actual cache/device limits. A reopened handle is not a cold OS cache. Keep real PostgreSQL JSON plans and use its normal planner to demonstrate multicolumn/covering behavior in a disposable temporary fixture.

The [guide](../INDEXES.md) and [protocol](../../benchmarks/storage/README.md) define the accepted claims and outstanding cold-cache/device/scale work. The full book inventory remains authoritative for final coverage.
