# Bounded encoding and compatibility lab

From the repository root:

```sh
make verify ID=C05-01
make encoding-demo
make encoding-bench
```

Or from `labs`:

```sh
go run ./cmd/lab encoding -runs 3 -iterations 16 -output ../artifacts/encoding.json
go test -race -count=1 ./ch05/encoding
go test -run '^$' -fuzz '^FuzzWirePreflight$' -fuzztime=8s -parallel=2 ./ch05/encoding
```

The complete suite requires Linux and a Go compiler for the isolated gob worker. Pinned generation supports Linux amd64 and requires curl, sha256sum, unzip and dpkg-deb; ordinary codec execution uses checked-in generated code. No database or daemon is needed.

Seven formats share an order-like synthetic record. Default reports retain 126 repeated measurements (1/10/100 lines × two versions × seven formats × three runs), 28 compatibility cases, 17 relays, eight generated Thrift RPC pairs and twelve semantic observations. Output files are replaced atomically after successful verification. Limits and timing boundaries are in the [guide](../../../docs/ENCODING.md); the lab does not publish new live business records or claim completed rolling-deployment support.
