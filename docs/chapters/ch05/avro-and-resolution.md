# Chapter 5: Avro and writer/reader schema resolution

`make verify ID=C05-02` runs the [Avro guide](../../AVRO.md), [Go laboratory](../../../labs/ch05/avro) and [immutable schemas](../../../contracts/avro). The fixtures continue the order-snapshot encoding experiments without migrating authoritative production event histories.

| Book source ID | Named concept | Concrete implementation |
|---|---|---|
| B05-009 | Avro | Actual binary values without per-field tags, generated structs/dynamic maps, independent wire vectors, native OCF null/deflate files. |
| B05-010 | The writer's schema and the reader's schema | Three actual writer layouts resolved against three reader schemas; direct latest-schema reinterpretation counterexample and exact full-schema lookup. |
| B05-011 | Schema evolution rules | Eighteen compatibility entries, twenty-six named rule cases, four lossy relays and nine identity/default observations; required/defaulted fields, aliases, promotions, unions, enums and fixed types. |
| B05-012 | But what is the writer's schema? | Raw full-digest metadata, standard single-object CRC lookup, ambiguous/missing failures and actual OCF embedded writer schemas with immutable manifests. Network negotiation stays in the later service-dataflow package. |
| B05-013 | Dynamically generated schemas | Runtime schemas from captured catalog columns with stable ordinals, nullable addition, int-to-long promotion and reverse rejection; compare schema-driven generated Go types with map-based encoding. |

Avro can omit repeated field names because a known writer schema supplies order and type. That space saving creates a schema-history dependency. A current Go struct, today's database catalog or a latest registry alias cannot establish which schema wrote old bytes. A file carrying its schema and a message carrying a lookup fingerprint have different availability requirements.

Reader defaults fill fields missing from the writer, while writer-only fields are skipped. Nullable unions carry branch indices; the reader remaps those indices rather than assuming unchanged order. A schema-compatible numeric promotion may still lose value precision. Field and named-type aliases act from the reader side. The pinned runtime's conservative whole-union compatibility check and exact-branch preference are recorded as implementation choices, rather than assumed universal interoperability rules.

Parsing Canonical Form strips aliases/defaults, so equal parsing fingerprints do not establish equal full semantic contracts. The append-only registry retains both identities; a cold registry can replay an archived OCF using its embedded full writer schema. Generated Go types cannot recreate all original defaults or identity metadata.

Complete operational schema rollout, backfill retirement and rollback windows remain in C05-03. Broader interoperability/logical schemas and registry/retention operations remain visible work in C05-02; the five source headings and every other book concept remain in the saved baseline inventory.
