# RDF, SPARQL and positive Datalog

The warehouse/supplier fixture from the [property-graph lab](GRAPHS.md) now has RDF and Datalog representations. Five backends answer the same reachable-vertex questions: Go adjacency, an indexed Go triple set, a Go Datalog evaluator, actual PostgreSQL recursive SQL, and Apache Jena Fuseki/TDB2 executing SPARQL. This is a modeling laboratory; inventory and shipment authority remain in the application contexts.

```sh
make verify-semantic
make verify ID=C03-04
go run ./cmd/guide verify-semantic
```

These are alternative entry points from the repository root. Go 1.27+, Make, Docker Compose, curl and sha512sum are required. Verification runs Go race tests and a Go report, builds the pinned RDF runtime, starts isolated PostgreSQL/Fuseki services, executes three native integration tests and the five-backend comparison, and stops those containers even when verification fails. CI runs the complete image profile and retains `artifacts/semantic.json` and `artifacts/semantic-services.json`.

```sh
make semantic-test semantic-demo    # Go models only; no services or credentials
make up-rdf                        # leaves the independent native profile running
go run ./cmd/guide verify-semantic-services
make down-rdf                      # stops its containers, retaining volumes
```

The profile binds PostgreSQL to loopback **18433** and Fuseki to **18303**, separate from the application and prior graph profiles. Override `ATLAS_RDF_POSTGRES_PORT` or `ATLAS_RDF_HTTP_PORT` consistently for Compose and the guide. The guide passes private connection settings through the child environment. `make configure` appends missing `ATLAS_RDF_PASSWORD` credentials to ignored owner-only `.env`, preserving existing credentials. Save that file with the profile volumes. The database administrator must be able to create disposable databases.

Fuseki **5.6.0** comes from its official Maven Central executable artifact, verified against the SHA-512 pinned in [the preparation script](../tools/prepare-rdf-runtime.sh). Its published Maven SHA-1 was also checked when recording that pin. The image pins the Eclipse Temurin 21 JRE manifest and runs as UID 10001 with a read-only root, writable `/data`, and a temporary private Fuseki base/password file. Runtime downloads happen before Docker build, so the build does not require Maven access. Health checks perform authenticated queries; an unauthenticated query receives 401. This loopback Basic-auth development profile does not supply production TLS, identity or authorization.

## One question, five representations

`Query` selects tenant, start, relationship kind (`all`, `route`, `supplies`), direction (`out`, `in`), minimum capacity and reflexivity. Only enabled edges meeting capacity qualify. Results are unique `{id, kind, name}` vertices sorted by ID, with no path, cost or minimum-hop projection. The previous graph lab owns those other contracts.

| Question | Required result |
|---|---|
| Reflexive closure, SPARQL `*` | Include an existing start at zero hops, then eligible reachable vertices |
| Non-reflexive closure, SPARQL `+` | Require at least one edge; a real cycle can still return its start |
| Isolated existing start | One vertex for `*`, none for `+` |
| Missing start or tenant | Empty, including `*`; never invent a zero-hop resource |
| Incoming traversal | Reverse traversal, preserving stored edge identity/direction |

The default is **128 vertices, 138 edges and 2,408 triples**, across two tenants with deliberately repeated logical IDs. Fourteen questions cover cycles, parallel edges, isolated/missing nodes, suppliers, reverse direction, capacity and a component extending far beyond six hops. Initial, property-update and vertex-delete phases each republish current data. Repetitions default to three, accept 1–5, and vertices accept even counts 32–512. The Go report has **378** measured samples; the native report has **630**, fifteen publication timings and four actual SQL plans. Each case first warms the backend; measured answers must match an independent edge-scan fixed-point oracle. Backend order rotates across cases.

```sh
cd labs
go run ./cmd/lab semantic -rows 64 -runs 1 -output ../artifacts/semantic-small.json
```

These commands keep Go state in memory; `-workdir` does not select a persistent RDF/Datalog store. Repetitions are query repetitions over each published fixture, not fresh native database builds. Warm client wall time includes each implementation's representation work, described below. It is not a general performance ranking.

## RDF identity and interchange

Logical warehouse identity is an absolute IRI such as `urn:atlas:tenant:acme:node:wh-000`. The other tenant has another IRI even when its node ID is also `wh-000`. Random run identity lives in named graph URIs, so logical entity identity does not change with the experiment. Vocabulary terms use `https://atlas.example/vocab#`; prefixes are local abbreviations, not identity. The lab never dereferences those IRIs.

Nodes have type, owner, ID, label and scalar properties. Every relationship is a resource with its own IRI, type, owner, from/to, cost, capacity and enabled values. Reification preserves parallel edge identities. RDF object literals distinguish strings, XSD integers/booleans and language-tagged text. In the Go term model `"01"^^xsd:integer` and `"1"^^xsd:integer` are distinct terms; a SPARQL numeric comparison can nevertheless equate their values. Native tests demonstrate this distinction with SPARQL constants; they do not certify every TDB2 numeric storage encoding or lexical preservation behavior. Fixture numeric values use canonical lexical forms.

The Go codec supports a bounded one-triple-per-line N-Triples subset: absolute unescaped IRIs, document-scoped ASCII blank labels, string/typed/language literals, RDF character/Unicode escapes and comments. It is not a Turtle or RDF/XML parser. JSON-style escaped slash, invalid Unicode scalar escapes, unsupported syntax, literal subjects and non-IRI predicates fail explicitly. Language tags are normalized to lowercase. Blank labels have identity only inside a document; importing another document with the same label creates another blank resource. Re-export can rename blank labels while preserving their relationships.

The Go store deduplicates triples and indexes subject, predicate and object separately, choosing the smallest bound candidate set and checking other bindings. These are in-memory hash indexes, not all physical SPO permutations or a durable database. Native Graph Store HTTP requests import actual Turtle, with a different vocabulary prefix, typed and language literals and independent blank-node scopes. This is the concrete semantic-web interoperability exercise: shared IRIs permit independent serialization/vocabulary use without matching display names. Ontology agreement, provenance, federation and RDFS/OWL inference remain separate concerns; there is no automatic inference here.

## Real SPARQL execution

Each run owns source named graphs for its tenants. A query creates a fresh scratch graph in Fuseki. Two SPARQL updates copy the selected owner's defined warehouse/supplier nodes and materialize direct `a:link` triples only for eligible relationships. The edge and both endpoints must have the expected owner. Checking named graph placement alone would allow a misplaced foreign edge or endpoint to affect traversal.

The result query uses `a:link*` or `a:link+`, joins the start to its actual node definition, and returns `SELECT DISTINCT` vertices and their scalar projections. The explicit start join prevents a missing IRI from appearing through property-path zero-length semantics. Direction and capacity are applied during link projection; an unconstrained property path followed only by endpoint filtering would answer another question.

Source RDF and Datalog facts are sets. SPARQL joins produce solution bags: two source edge resources with identical endpoints produce two endpoint solutions unless deduplicated. Native tests preserve this two-versus-one distinction. Membership comparison deliberately uses DISTINCT and projected set links, discarding relationship multiplicity only for this query contract.

Query wall time includes scratch projection, property-path evaluation, HTTP decoding and owned scratch cleanup. Publication time is recorded separately. Cleanup drops only owned named graphs, including a scratch graph retained after failed cancellation cleanup; it never clears the dataset. Source replacement and queries are serialized per client. This is not a public arbitrary-SPARQL gateway, a hostile-dataset validator or a cross-client snapshot protocol.

## Completed bounded Datalog evaluator

The [implementation](../labs/ch03/semantic-graph/datalog) evaluates positive, function-free, range-restricted programs. It parses facts and rules, checks predicate arities and variable safety, indexes relation columns, unifies constants/repeated variables, deduplicates facts, and computes the least fixed point. A synchronous reference evaluator and a semi-naive evaluator must agree. Delta evaluation joins each body position with facts added in the previous round, avoiding repeated evaluation over only old facts. It also handles mutual and nonlinear recursion.

```text
program   = { fact | rule } .
fact      = atom "." .                         # every argument is a constant
rule      = atom ":-" atom { "," atom } "." .
atom      = predicate "(" term { "," term } ")" .
term      = Variable | constant .
constant  = JSONString | "<" AbsoluteIRI ">" | Int64 | "true" | "false" .
```

Predicates are lowercase ASCII names of at most 32 characters; variables start uppercase, also at most 32. `#` begins a line comment. Every head variable must occur in the positive body. Strings and IRIs remain distinct typed constants even when their text matches; integer constants are canonical int64 values. There are no anonymous variables, function symbols, negation, arithmetic/comparison builtins, aggregates or disjunction. Unsupported features return an error; this is not an implementation of every Datalog dialect.

The route program derives `step(T,X,Y)` from eligible edges, allowed kinds, `capacity_ok(C)` and defined same-tenant endpoint nodes. The host explicitly supplies a finite capacity-eligibility relation; the evaluator does not secretly interpret arithmetic. It then derives closure:

```text
reachable(T,Y) :- start(T,X), step(T,X,Y).
reachable(T,Z) :- reachable(T,Y), step(T,Y,Z).
# Add this rule for reflexive closure:
reachable(T,X) :- start(T,X), node(T,X,K,N).
```

Function-free finite constants, bounded arities and set-valued facts ensure a finite possible ground relation. Positive rule application only adds facts, eventually reaching a fixed point even on cycles. That argument does not justify retaining old derived facts after deleting inputs. Each fixture/query rebuilds the current finite fact set and recomputes; no incremental deletion maintenance is claimed. Timing includes seed construction and the complete evaluation, and records rounds, derived facts and candidate rows.

## Bounds and remaining work

The shared fixture caps vertices at 512, edges at 2,048 and incoming/outgoing degree at four. RDF caps individual terms/IRIs, 32,768 triples per store, 8 MiB input and 64 KiB lines. Datalog caps source at 64 KiB/16,384 tokens, rules at 32, predicates at 64, arity at eight, body atoms at eight and variables per rule at 32. Default evaluation limits are 32,768 facts, 1,024 rounds and 2,000,000 candidate rows; explicit hard ceilings prevent raising them arbitrarily. Budget failure returns no partial result. Context cancellation is checked during evaluation.

The HTTP adapter caps request/response bodies at 8/2 MiB, limits result rows, refuses redirects and uses a 15-second client timeout. The profile sets a ten-second server query timeout and a 384 MiB Java heap. These bounds make the fixture practical; they do not establish production SLOs, query sandboxing or durable crash recovery. PostgreSQL and TDB2 provide their native storage; this lab has no custom durability engine.

Read the [chapter mapping](chapters/ch03/semantic-graphs.md), [decision](adr/0015-rdf-identity-and-positive-datalog.md), [runbook](runbooks/semantic-graph-lab.md) and [recorded validation](VALIDATION.md). C03-04 remains in progress for its full sub-concept audit, broader interoperability, scale and operational work. The original book inventory and all other packages remain the completion gate.
