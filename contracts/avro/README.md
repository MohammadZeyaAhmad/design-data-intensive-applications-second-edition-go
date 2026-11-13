# Immutable Avro snapshot schemas

These are laboratory contracts for synthetic order snapshots, with the same identity/value conventions as Atlas. They do not replace production event histories. Both the root application and lab module build independently; canonical `.avsc` sources live here and embedded/generated copies live in the lab module.

| Writer version | Change | Reader compatibility |
|---|---|---|
| v1 | Original field order, int quantity, defaulted nullable legacy note | v1, v2 and v3 can read it. |
| v2 | Reordered fields, removed legacy note, added currency/discount/labels with defaults | v1 and v2 work in both directions; v3 also reads v2. |
| v3 | Reader field alias, long quantity, reordered nullable union branches, different reader defaults | Reads earlier versions. Earlier readers reject v3 even when its quantity value fits int. |

All versions keep the Avro record fullnames `atlas.encoding.OrderSnapshot` and `atlas.encoding.Line`. Field names/reader aliases and actual writer order drive resolution. Namespaces, record names and field order are schema data; current Go type layout cannot stand in for an archived writer schema.

Run `make generate-avro` after intentional edits, review schema and generated changes together, then `make check-avro-generated`. The repository-owned generator emits ordinary Go structs for these finite record/array/map/nullable primitive fixtures. Encoding/resolution uses pinned `github.com/twmb/avro v1.9.1`, without a claim that this tool is a general Avro compiler.

The [guide](../../docs/AVRO.md) explains complete-schema identity versus Parsing Canonical Form, missing/ambiguous registry lookup, native OCF headers, captured catalog metadata and immutable dataset publication. Published versions must never be edited in place; register a new identity and verify both reading directions instead.
