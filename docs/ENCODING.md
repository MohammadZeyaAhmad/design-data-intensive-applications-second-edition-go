# Encoding and schema compatibility

C05-01 now has an executable comparison of gob, JSON, XML, MessagePack, Protobuf, Thrift Binary and Thrift Compact. It uses the same kinds of tenant/order/product identities as Atlas, with synthetic immutable snapshot fixtures in the independent Go lab module. Production event formats and command authority remain owned by their existing bounded contexts. Run from the root:

```sh
make verify ID=C05-01
make encoding-demo
make encoding-bench
go run ./cmd/guide verify-encoding
```

The suite requires Linux and Go. Reproducible generation currently supports Linux amd64, with curl, sha256sum, unzip and dpkg-deb. It downloads checksum-pinned protoc 36.2 and Thrift 0.24.0 to ignored artifacts and installs protoc-gen-go 1.36.6 there; it needs no root installation. Checked-in generated code permits ordinary lab builds without the native compilers. `make generate-encoding` updates sources; `make check-encoding-generated` rejects drift and exercises real compiler rejection of reserved Protobuf tag/name reuse.

## One record, explicit version choices

Both versions contain tenant/order IDs, a Unicode name, unique SKU/quantity lines, an int64 source version and optional expedited flag. Version 1 also carries a legacy note. Version 2 retires that note and adds currency, discount and labels. Explicit false/zero differ from absence. The default fixture includes version 9,007,199,254,740,993, beyond JavaScript's exact integer range.

Every decoder takes its reader version explicitly. The public boundary checks syntax, resource limits and domain validity before returning a record; incompatible values return an error and a zero record. A byte stream that parses is not automatically a valid order, authorized data or a valid command. The fixture's tenant string is data, without authentication authority.

Default runs retain **126 raw samples**, **28 writer/reader cases**, **14 domain-projection relays**, **two native generated Protobuf relays**, **one raw-object JSON relay**, **eight generated Thrift RPC combinations** and **twelve semantic probes**. Common fields survive all compatible matrix entries. Typed cross-version projections intentionally lose version-specific fields in both directions. The report checks these expected losses instead of marking every successful decode as lossless compatibility.

## What the formats teach

| Format | Executable lesson |
|---|---|
| gob | Go field/type metadata and stream type tables; native false pointers flatten to absence, so explicit presence wrappers are required here. Native int64/string field changes fail. |
| JSON | Names are the field identity. Schema openness controls acceptance; a typed relay still loses unknown fields. Bare float64 loses the fixture version, while decimal strings and `UseNumber` retain it. |
| XML | Element names and text represent the same domain with explicit repeated line/label elements. Maps require a chosen representation; strict bounded parsing rejects DTDs, namespaces/attributes and duplicate scalar siblings. |
| MessagePack | Binary encoding can retain JSON-like names without a tagged IDL. This map/string/integer/bool/nil profile rejects floats, binary values and extensions. Wider integers are checked before an old int32 target can narrow them. |
| Protobuf | Numeric tags allow the name at tag 3 to change in binary. Generated ProtoJSON names still change. Native generated relays retain unknowns, while domain projection loses them. Retired tag/name 5 are reserved. |
| Thrift Binary/Compact | Real old/new generated RPC clients/processors preserve common field IDs. Unknown fields are discarded, and default/presence behavior differs from Protobuf. Compact reduces some representation overhead without changing those semantics. |

Protobuf line quantity widens from int32 to int64. Both use varints, so native decoding of 4,294,967,297 into the old field produces 1. The public preflight rejects the quantity before narrowing. The separate Thrift wide schema changes the wire type from I32 to I64; both directions reject a missing required quantity even for value 1. The compatible Thrift v2 fixture therefore retains I32. Widening a source-language type alone does not establish safe compatibility.

Pinned Thrift Go generation represents optional currency with default USD as a value string. The getter returns USD when absent; explicit USD is considered unset and omitted on writing. The public decoder observes wire presence rather than treating the getter default as an explicitly written value. Optional expedited/discount are pointers and preserve false/zero. JSON accepts null and absence but ordinary pointers merge them; patch contracts requiring three states need a richer representation.

JSON Schema draft 2020-12 sources and an OpenAPI 3.1 component document live in [api/openapi/encoding](../api/openapi/encoding). The latter defines no deployed endpoint. The old schema is open to new properties; a probe closes it and rejects the same new fixture. Schema length counts, decimal patterns and structural checks supplement the domain's byte, integer-range, uniqueness and business checks. Compilers, schemas and generated code improve contracts and tooling; none replaces semantic validation or a rollout policy.

## Measurement boundaries

Every encode starts a fresh self-contained record, including gob type metadata. Warm passes precede measurements, schemas are compiled before timing, and helper compilation is excluded. Each sample records encoded bytes, a complete semantic fixture SHA-256, and separate encode/decode wall time, process CPU and parent allocation totals. Divide totals by `iterations` for per-operation observations. CPU uses Linux `getrusage` user+system time for the parent plus reaped children; background GC and other process work can affect it.

For version 2, this run's exact byte counts are:

| Format | 1 line | 10 lines | 100 lines |
|---|---:|---:|---:|
| JSON | 275 | 555 | 3,436 |
| XML | 452 | 930 | 5,791 |
| gob | 497 | 605 | 1,723 |
| MessagePack | 230 | 509 | 3,301 |
| Protobuf | 135 | 252 | 1,422 |
| Thrift Binary | 199 | 397 | 2,377 |
| Thrift Compact | 131 | 239 | 1,357 |

All paths include their preflight and domain checks; JSON also runs JSON Schema validation. Gob decode includes a fresh process, IPC and JSON return per record, while other decoders execute in process. Parent allocation counters exclude gob child memory and OS/kernel memory. Go benchmarks retain three additional repetitions with the same distinction. These boundaries answer a concrete local design question and do not establish an inherent speed ranking, physical storage cost, network throughput or a cross-language benchmark. Fresh gob records also do not measure a long stream's amortized type-table cost. Deterministic Protobuf output is not a universal canonical cross-language representation.

## Bounded input and operational limits

Public input is at most **64 KiB**, with at most **100 lines**, 1..1,000,000,000 quantity, positive signed-int64 versions, eight labels and UTF-8 byte limits. Shared text also excludes XML 1.0's forbidden control characters so an encoder cannot silently replace them. JSON preflight caps depth 8, tokens 2,048, object properties 32 and arrays 100; duplicate keys fail. XML caps depth 8 and tokens 4,096 before typed decoding. MessagePack scans declared counts/lengths before the native library allocates; maps cap at 32, arrays at 100 and depth at 8. Protobuf scans at most 512 fields and 100 lines, rejects groups and checks quantity before conversion. Thrift caps bytes, depth 8, 1,024 visited values, list/set length 100 and map length 32 before generated allocation. These are a bounded application profile, with deliberately restricted wire features.

Go documents gob as unsuitable for hostile inputs. It therefore decodes concrete types in a short-lived non-race Linux worker with **256 MiB RLIMIT_DATA**, **4 GiB RLIMIT_AS**, **2-second RLIMIT_CPU**, a **2-second wall timeout**, bounded input/output and a soft **64 MiB GOMEMLIMIT**. A test observes the real process's kernel limits and successfully decodes 100 lines under them. Failure kills/collects the worker and returns an error to the parent. These limits contain resource failures; they are not a filesystem/network/syscall sandbox or permission to deserialize arbitrary language-specific objects in a production service. Non-gob wire paths are fuzzed in process; malformed gob cases exercise the child boundary.

Truncation, trailing garbage, oversized frames, huge declared container counts, duplicate nested XML fields and invalid numeric widths have explicit tests. Some native formats permit concatenated/merged records, duplicate singular tags or map ordering differences by design; the lab's accepted profile is not a universal canonical framing protocol. No external entities, custom gob methods, interfaces or arbitrary type registrations are used.

## Remaining work

C05-01 remains in progress for its complete sub-concept audit, broader interoperability/scale and production mixed-version deployment operation. This increment starts P34's codec/schema portion; Avro resolution, registry selection, gRPC and full API governance remain tracked. Next is **C05-02 Avro and schema resolution**, using immutable actual writer schemas rather than an assumed latest schema. The original 88-package plan and every book heading remain present.
