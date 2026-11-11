# Chapter 3: GraphQL and API read composition

Book section **B03-025 — GraphQL** is exercised by the [read adapter](../../GRAPHQL.md), the [actual query fixtures](../../../labs/ch03/graphql) and `make verify ID=C03-05`. It also begins the GraphQL/gateway portion of P37; REST/OpenAPI/gRPC and complete API governance remain broader work.

GraphQL describes the response a client needs from an API. Its nested selection graph does not require graph database storage. This adapter composes the same PostgreSQL-backed Ordering/Catalog data through published contracts; the [property graph](property-graphs.md) and [RDF/SPARQL/Datalog](semantic-graphs.md) laboratories answer different data-model/query questions.

For each order line, a naive resolver or a client composing single-resource REST calls can perform another product lookup and details lookup. Collection prefetch gathers all selected identities first. The real engine then resolves fields from request-local futures. One, ten and one hundred line fixtures match complete REST answers with three GraphQL SQL statements, versus 3/21/201 for the measured baseline. This demonstrates N+1 avoidance, not an inherent database advantage of one API language.

Client-selected fields change the response shape but never select its tenant. Foreign identities are null or rejected at the relevant boundary; aliases/fragments cannot change the canonical permission used by a protected field. Query cost also follows expanded selections and list cardinality, not just depth. Expensive queries fail before source reads.

An eventual order projection and current Catalog price can legitimately describe different moments. The API exposes projection versions and the order's frozen accepted price separately. It does not turn composition into a transaction, event replay or a command precondition.

Signed identity-keyset cursors, precise decimal-string Long values, nullable field failures and a retained deprecated price field make the API contract's operational/evolution choices executable. Full schema rollout, field removal, OIDC roles and broader dashboards remain explicit gaps, with the original book inventory unchanged. The next planned package is C05-01 encoding and compatibility.
