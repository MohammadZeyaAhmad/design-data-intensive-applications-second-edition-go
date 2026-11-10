# Storage lab recovery

Run `make verify-storage` on Linux without starting application services. The scenario owns temporary databases and removes them after producing its report. Library users must give each database a private directory on a filesystem honoring the documented fsync/rename/lock contract.

| Result | Meaning | Action |
|---|---|---|
| `ErrLocked` | Another handle/process holds the advisory lock | Close the owning handle or wait for its process to exit; do not remove `LOCK` |
| `ErrRecovery` | A mutation returned an uncertain I/O/failpoint outcome | Close the poisoned handle, preserve all files, reopen with the stored style, and reconcile the last operation |
| `ErrCorrupt` | A complete frame, manifest or registered immutable file is invalid/missing | Preserve a copy for investigation; use a known good copy or regenerate a disposable lab dataset |
| `ErrLimit` | A documented key/value/table/parser bound was reached | Check options and dataset size; a wrapped recovery error still requires reopening |
| Incomplete final WAL frame | Interrupted append with a valid earlier prefix | Reopening truncates that tail and fsyncs the repair |
| Temporary or unreferenced SSTable | Interrupted flush/compaction cleanup | Successful recovery follows the manifest and removes only recognized unreferenced files |

Never remove the WAL, edit a checksum, choose the highest-numbered table as authority, or delete a tombstone to make a database open. Old runs can contain values that were subsequently deleted. A missing registered WAL is an error even when its expected current length might have been zero. A lock file stays in place across closes; unlinking it can let another writer acquire a different inode.

Synchronous nil-error writes have a durable-prefix promise under the stated storage contract. Asynchronous acknowledgments need a later successful `Sync` or checkpoint fence. Close and process death do not establish such a fence. An errored write may be present after recovery, so reissuing a counter increment requires application-level identity or transaction semantics beyond this API.

Evidence commands:

```sh
cd labs
go test -race -count=1 -run 'TestPowerLossAtEveryFilesystemMutation|TestRealProcessKillAtDurabilityBoundaries' -v ./ch04/lsm
go test -run '^$' -fuzz '^FuzzRecordDecoder$' -fuzztime=5s -parallel=2 ./ch04/lsm
```

The power-loss model starts with a durable parent directory and assumes atomic same-directory rename plus honest fsync. Actual process-kill tests retain the OS page cache. Controller caches, faulty storage firmware, filesystem bugs, remote filesystems and host power cuts are outside that evidence. Preserve those distinctions when reporting a result.
