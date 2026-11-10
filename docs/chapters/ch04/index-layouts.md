# Chapter 4: Storage trade-offs and index layouts

The [index guide](../../INDEXES.md) defines the implemented APIs, reports and limitations. `make verify ID=C04-03` runs the experiments below using warehouse snapshots. The original book inventory remains the final coverage gate; named section experiments do not certify every index occurrence in that section.

| Section | Executable experiment or bounded exercise |
|---|---|
| B04-011 — Comparing B-Trees and LSM-Trees | Identical mutation sets and query outcomes across tiered/leveled LSM, custom B+ tree and bbolt; record all configurations |
| B04-012 — Read performance | Separate hit, miss and bounded-range phases; record custom-engine bytes per query/result and warm point-read microbenchmarks |
| B04-013 — Sequential versus random writes | Sort by actual byte keys or use recorded shuffle seeds; rotate engine order and retain repetitions |
| B04-014 — Sequential Versus Random Writes on SSDs | Inspect reported filesystem/device identification; explain why container syscall counts cannot establish physical SSD behavior; device experiment remains open |
| B04-015 — Write amplification | Compare submitted bytes with WAL/page/table/manifest requests, including automatic maintenance; measure full compaction separately |
| B04-016 — Disk space usage | Compare logical file lengths with live data before/after LSM compaction; identify retained versions, free pages and excluded allocation overhead |
| B04-017 — Multicolumn and Secondary Indexes | Run leading-equality-plus-range tuple queries, atomic rekeys and tenant/prefix isolation; inspect PostgreSQL multicolumn plans |
| B04-018 — Storing Values Within the Index | Contrast full clustered rows, secondary locators and included projections; count primary-row fetches and inspect an index-only plan |
| B04-019 — Keeping Everything in Memory | Run the same queries over an ID map and ordered slice; disclose volatility, O(n) updates and the lack of an equivalent durability guarantee |

## Read less by ordering for a query

A row ID is a useful point-lookup key, but it does not put a tenant's available-stock interval together. A heap scan must decode all rows and sort matches. Adding the composite secondary key lets the engine seek to the tenant/warehouse/minimum and stop at the interval end or result limit. The remaining row fetches retrieve version and other projected data.

Placing version in the covering entry removes those primary-row fetches. A different query asking for unindexed columns would still need them. Placing the full row in clustered order also avoids a second lookup, but moving a row's ordering key moves its physical index entry. The lab keeps an ID locator for those updates. The natural warehouse/SKU uniqueness mapping is maintained in the same transaction.

These layouts use an ordered key structure; a hash map supplies fast exact ID lookup but no ordered range. The volatile layout therefore adds a sorted key slice and pays for insertion shifts. Keeping all data in memory does not remove the need to choose query structures or a durability/recovery strategy.

## Ratios need explicit accounting boundaries

Read-request bytes can be served entirely by the OS cache. Logical file length can exceed live payload even without a filesystem allocating every byte. An LSM flush can publish replacement files while old versions remain in other runs; full compaction changes both space and write cost. B+ tree redo and checkpoint writes can repeat whole pages for a short value. None of those ratios alone is device traffic.

Warm and reopened-handle measurements are published separately. Both retain the OS cache, and opening these engines validates data. Treating the second measurement as cold storage would give a misleading conclusion. Actual cold-cache/device validation remains an explicit package gap.

The PostgreSQL fixture's JSON plans demonstrate the optimizer's actual response to index changes, including sorting, buffer counters, scan kind and heap visibility checks. It does not force a scan with planner switches. Failure to obtain the expected plan is a failed experiment to investigate, not permission to label a forced plan as the normal outcome.
