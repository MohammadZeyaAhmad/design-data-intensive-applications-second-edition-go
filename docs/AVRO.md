# Avro and schema resolution

C05-02 adds real Avro binary encoding, writer/reader resolution, exact schema lookup and immutable OCF archives. It extends the [encoding experiments](ENCODING.md) using the same synthetic order identities and Go domain values. Live event stores, upcasters and bounded-context ownership retain their existing formats. Run from the repository root:

```sh
make verify ID=C05-02
make avro-demo
make generate-avro
make check-avro-generated
go run ./cmd/guide verify-avro
```

Go 1.27+ and Make are sufficient. The codec is pinned to `github.com/twmb/avro v1.9.1`; the lab pins its compression dependency to the application's existing `klauspost/compress v1.18.7`. The repository's bounded generator emits ordinary Go structs from the canonical `.avsc` files. Binary encoding/resolution still uses the same runtime for both generated structs and dynamic maps. No generated serialization engine or live schema-registry service is implied.

## Writer bytes and reader answers

Avro binary records contain values in writer-field order, without field names or numeric field tags in each record. A reader cannot decode those bytes using the latest schema's order. It first needs the actual writer schema, then resolves the reader's names, aliases, field order, defaults and promoted types against it.

Three immutable schemas retain the same record fullnames. Version 2 reorders fields, retires a nullable legacy note and adds currency, discount and labels with defaults. Version 1 and 2 read each other: old readers skip new fields, and a later writer missing the old note uses that old reader's null default. Version 3 renames the name field with a reader alias, promotes quantity int to long, reorders nullable union branches and changes reader defaults. It reads both older writers; the reverse direction is rejected, even for a small quantity. A writer's aliases do not update a previously deployed reader.

The same v1 bytes read with v2 acquire currency USD, while a v3 reader chooses EUR. Defaults belong to the chosen reader when the writer had no such field. They do not replace a null that was actually written. Explicit false and zero remain distinct from null after union branch remapping. A writer still emits the currency field when its value equals USD; a default annotation does not remove that field from its binary layout.

Default reports retain **18 matrix entries**: three writers × three readers × two Go representations. Four entries reject the v3-to-old direction; fourteen preserve every common field. Four v1/v2 read-modify-write relays retain common fields and deliberately lose version-specific metadata. Avro resolution skips unknown writer fields; those values cannot reappear from a typed or reader-shaped map projection.

Twenty-six named evolution cases cover defaulted/required addition and removal, field/record aliases, int/long/float/double promotion, string/bytes, unions, array/map item resolution, enum fallback and fixed widths. A permitted long-to-double promotion changes **9,007,199,254,740,993 to 9,007,199,254,740,992**. Schema compatibility alone does not prove value precision or business validity. A separate finite-domain check still validates identities, text, quantities, versions, labels and currency before returning records.

The pinned runtime makes two union choices explicit in the report: it checks that **every writer branch** can resolve before reading a datum, and it prefers an exact reader branch over a numeric promotion. Its eager whole-schema policy rejects a removed nullable branch even if a particular datum selected the remaining int branch. Other implementations/spec interpretations can defer branch failures until reading or choose unions differently. These observations describe the pinned implementation and must be included in a cross-language rollout audit.

## Finding the actual writer schema

| Dataflow | Implemented schema source | Failure behavior |
|---|---|---|
| Raw binary record | Explicit full writer-schema SHA-256 supplied as metadata | Missing writer identity fails before interpreting values; there is no latest-writer fallback. |
| Standard Single Object Encoding | `C3 01`, eight-byte little-endian CRC-64-AVRO, then payload; actual registry lookup | Unknown fingerprint fails; more than one full schema behind that fingerprint is ambiguous and fails. |
| Pinned single-object consumer | Caller supplies the full schema identity; the standard fingerprint must also match | The full contract disambiguates lookup; a mismatched header fails. |
| Archived dataset | Full schema embedded in a standard Object Container File, pinned by its manifest | Replay works with only the chosen current reader in memory; it does not need the historical registry entry. |

Parsing Canonical Form intentionally removes documentation, aliases and defaults. CRC-64-AVRO and SHA-256 of that canonical form therefore identify a parsing layout, without uniquely identifying the full semantic contract. The registry also hashes a normalized complete JSON schema, preserving field-array order and exact numeric tokens. A changed default has a different full digest even when both canonical fingerprints remain equal. Published version labels are append-only, returned schema bytes are copies, and concurrent lookups/encoding are race tested. Registry capacity is explicitly bounded at 32 versions; there is no mutable latest alias or eviction of archived contracts.

Inferring a schema from a generated Go struct is also different from retaining the original `.avsc`. The generated v2 field types do not reconstruct its authored currency/label defaults. The actual inference probe cannot read v1 using that inferred reader, while the authored v2 schema can. Generated types are useful code artifacts; they are not the schema history.

## Immutable archive files

The default scenario writes six actual OCF files: v1/v2/v3 × null/deflate codecs. Each contains three records with 1/10/100 lines, a full immutable writer schema, writer version/digest metadata and standard random sync markers. Each named dataset has a JSON manifest that binds the entire file SHA-256, writer identity, codec, bytes and record count. Files live at `objects/<sha256>.avro`; names live at `datasets/<name>.json` under a fresh retained `avro-archives-*` directory in the report output's parent.

Publication validates the archive and all business records, writes/syncs temporary files, then links them atomically with create-if-absent semantics. Identical byte retries succeed; a different payload cannot replace an existing dataset name. Twelve concurrent publishers produce one complete winner. A failed name publication can leave an unreachable content object. Replay verifies the complete file digest, schema metadata, counts and all records before returning an answer; corruption never returns a successful prefix.

The scenario creates a fresh registry containing only v3 and replays every archived dataset. Its historical writer comes from the embedded schema, with the reader's actual defaults/aliases applied. Retained files and manifests are useful inspectable artifacts, rather than a JSON summary of simulated archive behavior. Standard sync markers delimit blocks and are not integrity checksums; the manifest's whole-file digest provides that local integrity check. This is a private local-filesystem lab with tested ordinary operations, without a device power-loss, replicated retention or object-store WORM claim.

## Dynamic schemas from catalog metadata

The runtime schema builder consumes a captured list of SQL-style columns with stable source ordinals. It supports text/integer/bigint/boolean and nullable columns, rejects duplicate names/ordinals and unsupported types, and sorts by ordinal independently of input/map order. A later captured catalog adds nullable display_name and promotes quantity int to long. Old data reads with a null default and exact promoted value; the reverse direction fails.

This is an actual dynamically constructed Avro schema and map encoding, without a generated Go type for that catalog. The catalog fixture is not a live SQL extraction pipeline. A real exporter must persist the exact metadata snapshot and writer schema alongside each dataset; data must never be reinterpreted after querying today's catalog layout. Network schema negotiation belongs to the later service-dataflow package.

## Measurements and bounds

The default report has **54 raw samples**: three schema versions × generated/dynamic representations × 1/10/100 lines × three repetitions. Each has 32 iterations per path, exact payload bytes, complete semantic digests and separate wall/allocation totals. Warm passes and GC precede timing. Samples include input checks, bounded writer preflight, actual schema resolution and domain mapping. They exclude OCF publication/compression, disk, network and registry-server latency. These are wall/allocation observations, without process-CPU or universal speed claims. Map iteration need not yield canonical payload bytes.

Generated-mode v1 payloads occupy **105/186/1,034 bytes** for 1/10/100 lines; v2 and v3 each occupy **122/203/1,051 bytes** in this fixture. Field names are absent from payloads, so a rename alone adds no repeated name bytes. OCF files additionally carry full schema/header/metadata/block overhead and have separate recorded byte counts.

The accepted profile caps schemas at **16 KiB**, schema JSON depth 16/tokens 2,048, schema type depth 8, record fields 32 (64 total), union branches 8 and named-plan nodes 256. Numeric metadata is at most 32 characters with exponent magnitude at most 308. Logical types and recursive schemas are rejected before data decoding; fixed types and byte/string data cap at 256 bytes. Data caps at **64 KiB per raw record/block**, depth 8, 2,048 visited values, 100 array items and 32 map entries. Duplicate map keys and non-0/1 booleans fail this stricter input profile. Array/map blocks support both positive counts and negative counts with exact byte-length validation; int range is checked before conversion.

Owned OCF input caps at **1 MiB**, 128 records, eight metadata entries, 64 KiB compressed/decompressed per block and 1 MiB total decompressed data. Preflight checks the complete file, embedded schema, block counts, lengths, sync markers and every datum before native OCF decoding. Only null and raw deflate codecs are accepted. Tests exercise huge declared zero-byte arrays/block counts, a tiny deflate expansion beyond its limit, truncation, trailing bytes and corruption. Independent zigzag/string and positive/negative-array block vectors supplement actual runtime tests and bounded fuzzing.

C05-02 remains in progress for full sub-concept and cross-language audits, broader logical/recursive schemas, larger-scale exports and external registry/retention operations. The next planned package is **C05-03 expand/backfill/validate/contract and mixed-version schema evolution**, preserving the complete original scope.
