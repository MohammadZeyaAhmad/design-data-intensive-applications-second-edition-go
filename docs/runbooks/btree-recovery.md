# B+ tree lab recovery

Use `make verify-btree` without starting application services. The runner creates and removes only private temporary children. Library users must provide a private database directory with durable parents on the supported locking/fsync/rename filesystem contract.

| Observation | Interpretation and action |
|---|---|
| `ErrLocked` | Another handle/process owns `LOCK`; close/wait for it. Do not unlink the lock file. |
| `ErrRecovery` | A mutation has an uncertain outcome. Close the poisoned handle, preserve `CONTROL`, `DATA` and `WAL`, then reopen and reconcile the last request. |
| Valid nonempty WAL | Reopening validates and installs the metadata and page after-images, syncs DATA, then clears the log. |
| Interrupted recovery | Reopen again with the same files. A complete redo record remains until its data checkpoint is durable. |
| Incomplete WAL frame | It is discarded only if the existing data tree validates completely. |
| Corrupt complete frame, page or control | Preserve evidence and restore a known good copy or regenerate an explicitly disposable dataset. Do not edit checksums or guess a root. |
| Missing registered DATA/WAL | Fail closed. A formerly empty WAL is still a required file. |
| `ErrInvalid` for options | Keep the stored tree order; records must fit the documented key/inline-value bounds. |
| `ErrLimit` | The bounded page/image/depth capacity was reached; inspect the workload before choosing a larger engine. |

Never copy a WAL between databases, replay an older transaction over a newer generation, clear a pending WAL to open damaged pages, or place a live page on the free list. The decoder checks database identity, generation and image bounds. `Check` verifies that every allocated page is reachable or free, with no overlap or cycle.

```sh
cd labs
go test -race -count=1 -run 'TestPowerLoss|TestRealProcessKill' -v ./ch04/btree
go test -run '^$' -fuzz '^FuzzPageDecoder$' -fuzztime=5s -parallel=2 ./ch04/btree
go test -run '^$' -fuzz '^FuzzRedoDecoder$' -fuzztime=5s -parallel=2 ./ch04/btree
```

Model tests cover early/torn page persistence and repeated crashes during recovery. Process tests cover 18 named checkpoint/recovery cases, including separate leaf/sibling/branch writes. They retain the OS page cache and do not certify a physical power cut. Free pages are reused but the file is not automatically shrunk; deletion is not a secure-erasure or backup-retention implementation.
