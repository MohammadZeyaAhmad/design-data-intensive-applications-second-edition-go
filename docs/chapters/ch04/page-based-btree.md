# Chapter 4: Pages, separator keys and reliable tree changes

Package C04-02 adds a B+ tree to the same synthetic warehouse snapshot comparison as the [LSM lab](log-structured-storage.md). Run `make verify-btree` and inspect `artifacts/btree.json`; use `make storage-demo` to compare all five stores. The [tree guide](../../BTREE.md) defines the actual API, format and limits.

| Book section | Named experiment |
|---|---|
| B04-008 — B-Trees | Grow a multi-level tree with inserts, check ordered range scans, borrow/merge on deletion, collapse its root, reopen and reuse the freed pages |
| B04-009 — Making B-trees reliable | Crash before/after the WAL fence, during leaf/branch/metadata installation and while recovering; compare the recovered generation and full map with the acknowledged prefix |
| B04-010 — Variants | Compare leaf-only B+ tree values with B-tree placement, in-place redo with bbolt copy-on-write, and the implemented coarse lock with page latches and snapshot designs |

## A separator is a routing fact

A parent separator is the smallest key in the child immediately to its right. Inserting a new minimum can change an ancestor even without splitting a page. Borrowing from a neighbor also changes a separator. The implementation derives separators from child minima rather than leaving stale copied keys after those operations.

An overflowing leaf splits into two sufficiently occupied leaves and updates its forward link. An overflowing branch splits its child list, then reconstructs both separator arrays. Root overflow creates a new branch. Deletion borrows when a neighbor can spare an entry/child; otherwise it merges, frees the redundant page and may propagate underflow upward. A root with one remaining child collapses to that child.

The reference-model tests use orders 4, 6 and 8 and random updates, deletes and ranges. `Check` runs after mutations to verify key order, occupancy, common leaf depth, exact separator minima, sibling links and complete live/free page accounting. A separate grow-delete-reopen-reinsert schedule checks that the old file allocation can be reused.

## Atomicity spans several pages

A split can change an existing leaf, create a sibling and root, and change metadata. A merge can change a sibling link, a parent and the free-list head. Syncing individual pages in arbitrary order cannot make that group atomic. Full-page redo supplies a complete intended state before any data overwrite is allowed.

The commit fence is the WAL fsync; the checkpoint fence is the later DATA fsync. Clearing the WAL requires the second fence. A checksummed metadata after-image is included, so recovery can start even when page 0 is torn. The recovery overlay validates all intended changes against untouched pages before applying the redo record. Replaying those after-images is safe after another crash.

The shared `labs/internal/diskmodel` separates stable inode contents from stable directory entries and can expose partially overwritten pages before fsync. Each modeled cut must recover an allowed submitted prefix at least as long as the acknowledged prefix, with a structurally valid tree and identical values. Native process tests separately kill workers during a root split and during recovery; their page cache survives.

## Locks and alternative page designs

This engine holds one mutex over a whole operation and an advisory lock over the database directory. It therefore has no concurrent in-process page installation/read race. A production latch design can lock pages briefly and release safe ancestors while descending; splitting and merging require consistent acquisition rules and careful sibling coordination. Those fine-grained latches are not implemented here.

Copy-on-write instead writes replacement pages and publishes a new root after syncing them. Readers can retain old roots when the engine manages snapshot lifetimes and delayed free-page reuse. bbolt supplies that mature baseline. This tree updates pages in place behind full-page redo and returns copied method results, without MVCC or long-lived read snapshots. Fixed page size alone does not establish a concurrency or durability guarantee.

The report records the low fanout and inline limits, sync policy, filesystem, raw timings and page/WAL counters. Its warm-process workload does not identify physical device latency or complete C04-03's controlled performance experiments.
