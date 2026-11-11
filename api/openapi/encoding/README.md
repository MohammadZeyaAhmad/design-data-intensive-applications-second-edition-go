# Encoding contract fixtures

These JSON Schema draft 2020-12 documents describe synthetic order snapshots used by the [encoding lab](../../../labs/ch05/encoding). The OpenAPI 3.1 file publishes schema components with an empty `paths` object; it describes no deployed HTTP operation. Production event envelopes, REST handlers and the GraphQL schema retain their existing contracts.

Both versions require tenant/order identities, name, lines and a decimal-string version. Version 1 has a retired legacy note; version 2 adds currency, discount and labels. Unknown properties are allowed for compatible readers. A separate executable probe closes the old schema and demonstrates why rejecting additional properties changes compatibility. Schema acceptance alone does not retain those fields in a typed Go relay.

JSON Schema string lengths count characters, whereas the lab's Go domain imposes UTF-8 byte bounds too. The schema's decimal-string patterns do not alone enforce signed-int64 range or the discount business limit. The public decoder applies both checks, rejects duplicate keys, and scans bounded tokens/depth before schema validation. Labels and optional scalars accept null; typed pointers can merge null with absence. Records requiring three-state patch semantics need a different contract.

Run `make generate-encoding` after editing these files; the embedded copies in the independent lab module are checked for drift by `make check-encoding-generated`. See the [guide](../../../docs/ENCODING.md) for the different binary-schema rules.
