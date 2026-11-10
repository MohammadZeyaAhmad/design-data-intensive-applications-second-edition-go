# ADR 0011: Compressed segments with transactional delta and cube

Status: accepted, 10 November 2026.

The analytics increment needs independently testable column compression/pruning and query execution, while preserving updates/deletes and materialized results. Introducing another page engine would obscure those boundaries and duplicate the existing LSM/B+ tree recovery work.

Store the custom checksummed segment blobs in bbolt, together with exact version/location heads, a row-oriented delta and all tenant-preserving warehouse/category/day rollups. Publish ingestion, flush, full compaction and cube rebuild through synchronous transactions. Resolve visibility before row filtering. Retain tombstone version floors after compaction.

Implement scalar AST interpretation and mask-based column batches in process. Generate native Go predicates only from a bounded typed grammar and execute them through a bounded subprocess protocol. This makes actual generated code reviewable without loading user-supplied Go/plugins into the process. Compilation and protocol/startup costs remain explicit; the lab claims neither an in-process JIT nor explicit SIMD.

Keep frame bytes, metadata, delta JSON, process CPU and helper CPU separate. A row-JSON baseline is a representation/oracle comparison, not a matched decode or device-I/O benchmark. Cache/cloud/device claims require their own experiments. The [guide](../COLUMNSTORE.md) and [cloud design exercise](../chapters/ch04/cloud-warehouse-design.md) define the current and remaining scope.
