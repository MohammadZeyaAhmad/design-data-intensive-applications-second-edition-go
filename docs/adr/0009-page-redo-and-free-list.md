# ADR 0009: Full-page redo for the bounded B+ tree

Status: accepted for the ninth implementation increment.

Implement the custom B+ tree in the independent lab module, over the same warehouse dataset and common key/value comparison contract. Keep production PostgreSQL authority. Extract the low-level file/lock interface and deterministic fault model so the LSM and B+ tree protocols run under the same storage assumptions; their WAL and publication protocols remain distinct.

Use 4 KiB checksummed pages, a small persisted even order, leaf-only inline values and exact right-child-minimum separators. Maintain a persistent singly linked free list. Splits, merges, root changes, sibling links and free-list edits form one synchronous mutation.

Build metadata plus full page after-images in memory, sync the complete redo transaction, install pages with metadata last, sync DATA, then truncate/sync the WAL. Read the WAL before trusting metadata during recovery. Validate the intended tree through an overlay before installation and keep redo restartable. Reject stale/foreign records and incomplete logs over invalid data.

Exclude other processes with an advisory lock and serialize methods with one mutex. Return copied values, without fine-grained page latches, MVCC or long-lived read snapshots. Full-page redo makes torn-write recovery explicit but writes more log/data bytes than a compact physiological log. bbolt remains the mature copy-on-write baseline.

Validate both values and structure under reference-map schedules, allocation churn, modeled power loss, real process kills and page/redo fuzzing. Inline overflow pages, denser layouts, advanced concurrency variants and broad format/scale work remain further work, so C04-02 stays in progress.
