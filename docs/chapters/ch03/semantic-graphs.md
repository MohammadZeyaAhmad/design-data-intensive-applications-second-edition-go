# Chapter 3: Triple stores, the semantic web and recursive relations

Run `make verify ID=C03-04` using the [semantic graph guide](../../SEMANTIC_GRAPHS.md). This continues the same warehouses/suppliers as [property graphs](property-graphs.md), with equivalent membership queries across five representations.

| Book source | Concrete exercise and evidence |
|---|---|
| B03-020 — Triple Stores and SPARQL | Resource/literal triples; indexed Go set versus real Fuseki/TDB2; five-backend membership parity |
| B03-021 — The Semantic Web | Stable tenant-qualified IRIs; independent Turtle prefix/serialization import; document-local blank nodes; explicit ontology/inference/federation boundaries |
| B03-022 — The RDF data model | IRI/blank/literal distinctions, scalar datatypes, language tags, lexical identity, parallel edge reification, bounded N-Triples interchange |
| B03-023 — The SPARQL query language | Actual joins, property filters, named graphs, DISTINCT versus bags, `*`/`+`, missing-start guard, reverse/filtered path projection and HTTP protocol |
| B03-024 — Datalog: Recursive Relational Queries | Documented grammar, typed constants, range restriction, indexed unification, naive/semi-naive fixed points, mutual/nonlinear recursion, set semantics and language/budget rejection |

RDF can describe a string property or another entity with the same triple shape. A relationship resource is necessary here to keep two parallel routes with different attributes distinct. Choosing a shared IRI permits independent producers to refer to one entity; choosing matching display text does not. A semantic-web format alone does not resolve vocabulary disagreements or authorize retrieval from that IRI.

Reachability is a recursive relation, not a path enumeration problem. SQL `UNION` over only vertex identity and positive Datalog's deduplicated derived facts both reach a finite fixed point. Adding an unbounded depth column to the SQL recursive state would change the termination argument. The SPARQL engine evaluates a property path over the same eligible link relation. A cycle terminates for membership while still making its start a valid `+` result.

RDF term identity and expression value equality answer different questions. The Go model retains lexical forms; native SPARQL tests compare two typed numeric constants with `sameTerm` and numeric `=`. Triple sets and Datalog relation sets also differ from SPARQL solution bags. Comparing membership requires a deliberate DISTINCT contract, rather than assuming every engine deduplicates joins identically.

Monotone positive inference is safe when adding facts. A deleted warehouse can invalidate previous derivations, so the scenario rebuilds and reevaluates all current facts. Incremental retraction would need another maintenance algorithm. Negation, aggregation and function symbols have different semantic/termination obligations and are rejected by the bounded evaluator rather than approximated.

The original source inventory is unchanged. This scenario gives named evidence for these sections; it does not declare every sub-concept, production feature or the full chapter complete. GraphQL/read composition is the next separate package, C03-05; its API selection graph is distinct from a database graph model.
