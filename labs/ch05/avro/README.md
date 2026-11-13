# Avro writer/reader resolution lab

From the repository root:

```sh
make verify ID=C05-02
make avro-demo
make check-avro-generated
```

From `labs`:

```sh
go run ./cmd/lab avro -runs 3 -iterations 32 -output ../artifacts/avro.json
go test -race -count=1 ./ch05/avro ./internal/avroprofile
go test -run '^$' -fuzz '^FuzzBoundedAvro$' -fuzztime=8s -parallel=2 ./ch05/avro
go test -run '^$' -fuzz '^FuzzBoundedContainer$' -fuzztime=8s -fuzzminimizetime=100ms -parallel=2 ./ch05/avro
```

The default report retains 54 raw measurements, 18 generated/dynamic compatibility entries, four relays, 26 evolution rules, nine identity/default observations and a captured-catalog dynamic-schema exercise. Six real null/deflate OCF datasets remain in a fresh `avro-archives-*` child beside the output report. Each includes three records (1/10/100 lines), the full writer schema and a content-addressed manifest. Repeated runs create separate directories.

Go is sufficient; no database, registry server, Java compiler or native codec toolchain is required. The in-process append-only registry performs real fingerprint/full-digest lookup, with no HTTP registry operation claim. See the [guide](../../../docs/AVRO.md) and [runbook](../../../docs/runbooks/avro-replay.md) for finite input limits, conservative runtime union behavior and replay operations.
