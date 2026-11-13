# RPC process laboratory

From the repository root, run `make verify ID=C05-04`. For just the process drill, use `make rpc-demo`; the guide loads the existing ignored local test binding. The standalone lab command is:

```sh
cd labs
go run ./cmd/lab rpc -binaries ../artifacts/rpc-binaries -output ../artifacts/rpc.json
```

This command requires an inherited `ATLAS_TEST_DATABASE_URL` identifying the local `atlas_test` database. It creates a separate owned database, migrates through the actual Atlas executable, starts actual HTTP/two RPC/proxy processes, and cleans up its processes/database on success or failure. Ports are dynamically selected on loopback. Executable hashes must match the build manifest. The lab module does not import production internals.

Eighteen observations cover OpenAPI response conformance, four current/baseline client/server combinations, unknown-field preservation/loss, shared REST/gRPC receipts, health-aware balancing, a reply lost after confirmed commit, changed-payload conflict, a distinct-key counterexample, actual endpoint SIGKILL during a PostgreSQL lock wait, orphan-query limits, discovery recovery, cancellation/deadline propagation and complete withdrawal. Timings and transport statuses come from the native operations.

The baseline server is a frozen-descriptor adapter delegating to the real backend, not a historical release. Fault controls exist only in this lab's loopback services. The normal backend and proxy expose none. Reports retain generated-build provenance and explicit limits; this finite test does not establish a complete mesh, registry consensus, cross-language interoperability or production receipt retention.

See the [service guide](../../../docs/RPC.md) and [chapter mapping](../../../docs/chapters/ch05/rest-rpc-discovery.md).
