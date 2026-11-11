# Encoding compatibility verification

Run `make verify ID=C05-01` from the repository root on Linux amd64. It checks source coverage, reproducible generation, race tests, the default report and repeated Go benchmarks. No service credentials, Docker daemon or database is required. Reports are ignored files at `artifacts/encoding.json` and `artifacts/encoding-go-benchmark.txt`.

On a generator download failure, check outbound access to the official GitHub Protobuf release, Debian package mirror and Go module proxy. Keep pinned versions/checksums; do not substitute an unrelated compiler or bypass digest checks. Native generation needs curl, sha256sum, unzip and dpkg-deb. An unsupported platform fails explicitly; checked-in generated code can still compile where supported by Go, while the complete gob/CPU suite requires Linux.

After intentional schema edits, run `make generate-encoding`, review IDL and generated changes together, and stage the expected generated files before running `make check-encoding-generated`. The check compares against the Git index/worktree and rejects untracked output too. Never reassign reserved Protobuf tags/names or retired Thrift IDs. Thrift retirement history is a review record, without compiler enforcement.

For input failures, retain only a synthetic reproduction and its chosen format/reader version. A malformed wire, missing required business data, numeric overflow or exceeded count/byte/depth budget is an explicit error. A gob worker killed by memory/CPU/wall limits is also an error; do not retry unbounded decoding in the parent. The worker is built once per suite and its temporary directory is removed on normal close. A forced parent kill may leave a task-owned `atlas-encoding-worker-*` directory in the system temp directory; remove only that known abandoned directory.

For a report mismatch, inspect the complete fixture digest, common-field equality and metadata-preserved flags. Cross-version typed metadata loss is intentional and asserted. Generated Protobuf relays and the raw JSON object preserve it in their named cases; generated Thrift cross-version relays discard it. A successful parse with unexpected metadata is a failure of the contract, even if common fields match.

Measure without race detection for timing, retain raw repetitions and divide totals by iterations. CPU includes waited-for gob children; parent allocations exclude child heaps. Gob process startup/IPC and JSON Schema checks are part of their respective paths. Compare the stated work boundaries before interpreting speed or allocation figures. Run bounded fuzzing with `cd labs && go test -run '^$' -fuzz '^FuzzWirePreflight$' -fuzztime=8s -parallel=2 ./ch05/encoding`; native gob malformed-input checks use the worker instead.

This scenario does not change production serializers, restore event histories, register a deployed schema or test mixed-version rollout. Those actions require their own concrete contracts and acceptance scenarios.
