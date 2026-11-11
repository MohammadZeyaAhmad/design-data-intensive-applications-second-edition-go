# Versioned Thrift encoding fixtures

Versions 1 and 2 retain field IDs and line quantity i32. Version 2 renames field 3, retires 5 and adds optional currency, discount and labels. Both Binary and Compact protocols run through actual generated Go clients/processors in every client/server pairing over a bounded in-memory transport. This exercises RPC serialization, with no socket, TLS, deadline propagation or retry claim.

The `wide` schema intentionally changes required quantity to i64. Both read directions fail even for 1: the wire field type changes, the reader skips it, and generated required-field validation fails. This differs from Protobuf's varint narrowing. Do not infer compatibility from similar IDL syntax.

With Thrift 0.24.0's Go generator, optional currency with default USD is a value string. An absent field returns USD from the native getter, and an explicitly supplied USD is omitted by `IsSetCurrency`. The bounded public decoder records wire presence and returns nil when absent. Explicit false expedited and zero discount use pointers and survive. Other generators, versions or languages can differ; the report records this actual pinned behavior.

Unknown fields are skipped and discarded by these generated readers. Both cross-version RPC relays lose version-specific metadata. Thrift has no Protobuf-style `reserved` declaration; [field-history.json](field-history.json) records the retired ID/name, and they must never be reassigned. The fixture history is a review obligation, not an automatic compiler guarantee.

The generator is the pinned official Thrift 0.24.0 Debian compiler package, extracted into ignored user-owned artifacts and SHA-256 checked. The Go runtime is pinned to 0.24.0. Run `make check-encoding-generated` to reproduce all generated clients and types.
