# Chapter 5: encoding formats and schema compatibility

`make verify ID=C05-01` runs the [encoding guide](../../ENCODING.md), [Go laboratory](../../../labs/ch05/encoding), [JSON/OpenAPI components](../../../api/openapi/encoding), [Protobuf](../../../contracts/proto/encoding) and [Thrift](../../../contracts/thrift/encoding) fixtures. These synthetic order snapshots connect to Atlas's existing identity/value conventions; they do not replace live event contracts.

| Book source ID | Named concept | Implementation or decision exercise |
|---|---|---|
| B05-001 | Formats for Encoding Data | Seven equivalent codecs; repeated sizes, CPU, wall time and allocation samples for complete records. |
| B05-002 | Terminology clash | Translate values to bytes and back; distinguish encoding/serialization/marshalling, encryption and compression below. |
| B05-003 | Language-Specific Formats | Actual Go gob metadata, native type-change/zero-presence probes and a constrained child decoder. |
| B05-004 | JSON, XML, and Binary Variants | Unicode/number/presence fixtures; XML element/maps choice; MessagePack map representation and bounded binary parsing. |
| B05-005 | JSON Schema | Draft 2020-12 validation; tolerant versus closed schemas, null/absence and separate semantic checks. |
| B05-006 | Binary encodings | MessagePack versus tag-based Protobuf and Thrift Binary/Compact; raw bytes and process CPU with explicit boundaries. |
| B05-007 | Protocol Buffers | Real generated Go v1/v2 types, optional scalars, native unknown retention and Protobuf/Thrift comparison. |
| B05-008 | Field tags and schema evolution | Bidirectional writer/reader and relay matrices, reserved number/name compiler rejection, rename/narrowing/Thrift type mismatch. |
| B05-014 | The Merits of Schemas | Checked IDLs, compiled JSON Schema, OpenAPI components, generated RPC clients and reproducible drift checks; rollout and semantic limits. |

Encoding, serialization and marshalling often name the same values-to-bytes boundary, with usage varying by platform. In other contexts encoding means a character representation such as UTF-8. Encryption adds confidentiality through a cryptographic protocol, and compression removes representation redundancy; the current measurements do neither. Thrift Compact is a binary protocol choice rather than a general-purpose compression stage.

Human-readable names, stable numeric tags and Go-specific type descriptors express different contracts. An open JSON schema or a successful binary parse establishes neither semantic validity nor unknown-field preservation. A rolling relay can accept a new field and silently erase it on the way back. Typed Protobuf preserves unknown bytes only while they remain in the generated message; the same data loses them through a domain projection. Thrift's generated readers discard them immediately.

Schemas make expected shape, field identity and code generation reviewable. They can help compatibility tooling and compact encoding, while requiring governance for retired IDs, unknowns, defaults, presence and actual numeric ranges. Language-specific deserialization may invoke custom runtime behavior; this fixture limits gob to concrete owned structs and contains decoder resource failures in a child. It does not generalize safety to Java/Python or arbitrary gob objects.

The next package, C05-02, covers Avro writer/reader resolution, defaults, unions, promotions and immutable schema lookup. Storage/network deployment and registry operation remain distinct obligations in the complete saved plan.
