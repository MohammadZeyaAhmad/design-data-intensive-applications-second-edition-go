# ADR 0015: Stable RDF identity and bounded positive inference

Status: accepted, 11 November 2026.

The route/supplier domain already has graph identity and query contracts. To exercise C03-04, equivalent data must execute in a real RDF service and a completed small Datalog implementation, without treating syntax examples as implemented engines.

Keep stable tenant-qualified resource IRIs and put random run isolation in named graph URIs. Reify edges to retain parallel relationship identity. Use authenticated pinned Fuseki/TDB2 through SPARQL and Graph Store HTTP. Materialize a query-specific eligible-link graph with edge/endpoint ownership checks before evaluating `*`/`+`. Return DISTINCT membership and explicitly require a defined start. Each client serializes source replacement against its queries and cleans only its own graphs.

Implement positive function-free Datalog with explicit AST validation, fixed arities, range restriction, typed constants, column indexes and fact sets. Compare synchronous reference evaluation with semi-naive delta joins, including nonlinear/mutual recursion. The host supplies finite capacity eligibility as facts; no implicit builtin changes the documented language. Rebuild from current inputs after deletion. Language features outside this subset and exceeded resource budgets fail explicitly without partial results.

Reuse the prior Go property graph and PostgreSQL node/edge storage. The membership SQL uses recursive UNION over vertex ID alone, independent of the prior minimum-hop/path contracts. Check all five backends against an independent scan oracle through updates/deletion, and retain actual SQL plans and raw query/publication timings. Do not equate different engines' internal work or infer a universal speed ranking.

This completes a bounded language evaluator and native RDF comparison, not a production SPARQL gateway, all Datalog dialects, RDFS/OWL inference, multi-client publication, arbitrary-dataset validation or incremental fact retraction. Existing DDD contexts retain their authority and the full 88-package plan remains in force.
