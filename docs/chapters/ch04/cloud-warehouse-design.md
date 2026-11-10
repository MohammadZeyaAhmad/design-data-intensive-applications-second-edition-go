# Worked design: warehouse facts with separate storage and compute

The running lab is one local bbolt file. This exercise maps its correctness boundaries to a warehouse whose immutable data lives in object storage and whose query workers can be replaced independently. It is a design exercise, not a deployed cloud profile or a vendor-performance claim.

## Publish a complete snapshot

An ingestion coordinator consumes versioned warehouse facts and accumulates a durable delta. A segment writer produces immutable objects with row-group statistics and checksums. It uploads all new objects before attempting to publish a catalog generation that identifies the complete active set, schema/sort descriptors and version/delete information.

Catalog publication needs compare-and-swap or a transactional metadata service. A worker must never guess the active snapshot by listing filenames: an upload can succeed before publication, a listing can be paginated, and an abandoned upload is not an accepted table generation. A failed compare-and-swap leaves unreferenced objects for later collection, rather than making a partial generation readable.

The local lab obtains this boundary from one bbolt transaction containing segment blobs, heads and metadata. Moving the blobs to an external store would remove that shared transaction. Upload completion, catalog fencing and orphan cleanup therefore need a new tested protocol; changing a storage interface alone would not supply it.

## Query one pinned generation

A query coordinator pins a catalog generation and divides candidate row groups among workers. Workers fetch only required metadata/column ranges, apply conservative pruning, resolve the selected generation's version/delete state and evaluate predicates. Worker replacement repeats a task with the same generation and task identity, so partial results can be retried without double counting.

Parallel local aggregates can merge sums and nonnull counts. They cannot merge arbitrary averages by averaging them, and they must preserve explicit grouping/null markers. Joins and repartitioning can require shuffles, skew handling and spill; none of those mechanisms is supplied by this local column reader. Its mandatory tenant condition must also remain a coordinator/worker authorization boundary rather than an optional query convention.

Compute isolation allows independent query workgroups and resizing, but metadata, object requests, local caches, shuffle and memory budgets still constrain throughput. Cold and warm object-cache results require separate measurements. A column-frame counter in this lab does not measure cloud request amplification, remote bandwidth, storage billing or worker CPU isolation.

## Retire old state safely

Compaction publishes replacement objects in a new generation. Old generations remain readable until all pinned readers and recovery/replay horizons release them. Only then may collection remove their objects and obsolete delete/version information. A lease expiry alone needs fencing to prevent a paused worker from resuming against already-collected state.

Late facts, changes to dimension attributes, schema evolution, event-time windows and privacy/retention policies need explicit semantics. The synthetic fact format fixes a small schema and preserves tombstone floors; it does not implement a dimensional master-data service or lakehouse table specification.

The implementation backlog retains distributed catalog/object publication, task scheduling, joins/shuffles, cache/device trials and production event ingestion as remaining work. The [local experiments](column-analytics.md) cover the codec, visibility, predicate and aggregate rules that those distributed mechanisms must preserve.
