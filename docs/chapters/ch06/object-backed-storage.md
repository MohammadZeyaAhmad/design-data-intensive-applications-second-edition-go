# B06-005: Databases Backed by Object Storage

The C06-02 lab makes the separation of storage and compute executable for warehouse read snapshots. Immutable sorted segments and versioned manifests live behind an object-store port; independent Go workers publish by conditionally changing one current-generation pointer. Compute restart does not require copying an entire local database, while caches remain disposable derived state.

| Concept | Executable evidence |
| --- | --- |
| Immutable object data and published generations | Content-addressed segments/manifests; full size/hash/schema checks; head CAS after every reference exists |
| Conditional writes and concurrent compute | One winner from sixteen conditional creates/replacements; independent same-generation writers report one CAS conflict |
| Complete snapshot reads | Independent reader observations across publication; missing/corrupt reference rejection without partial rows; pinned original generation after compaction |
| Disposable compute and recovery | Actual process SIGKILL at four publication boundaries on both backends; original receipt reconciliation and immutable segment reuse |
| Reader caches and storage/API cost | Finite LRU over verified immutable content; fresh head GETs; cold/warm/reopened request and byte counts plus actual S3 HTTP counters |
| Consistency and service boundaries | Strong per-key GET and atomic conditional PUT are explicit port requirements; listing never establishes publication or authorizes online deletion |

The local adapter supplies actual file durability/process coordination; the memory adapter supplies deterministic schedules. The AWS SDK adapter and Moto 5.2.5 exercise actual S3 HTTP operations. An explicit real-bucket contract target uses the same suite, without claiming AWS validation from emulator results. Logical warehouse snapshots remain separate from authoritative Inventory reservation decisions.

The [guide](../../OBJECT_STORAGE.md), [implementation](../../../labs/ch06/object-storage), [scenario](../../../scenarios/object-backed-storage.json) and [validation](../../VALIDATION.md) record bounded behavior. This scenario starts C06-02; it does not close its full source-heading, operational, format, lifecycle or scale audit. All original plan obligations remain intact.
