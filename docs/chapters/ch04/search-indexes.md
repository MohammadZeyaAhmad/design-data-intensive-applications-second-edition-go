# Chapter 4: Multidimensional, full-text and vector indexes

The [search guide](../../SEARCH.md) describes the implementation and commands. `make verify ID=C04-05` runs the Go structures and real PostgreSQL/pgvector experiment. These section-linked exercises retain the full source inventory as the final coverage gate.

| Source section | Executable experiment |
|---|---|
| B04-028 — Multidimensional and Full-Text Indexes | KD rectangle/radius results match an independent scan; wrapped longitude and polar cases preserve membership; application point/box queries use GiST-capable predicates |
| B04-029 — Full-Text Search | Inverted positions, title weighting, tenant-local ranking, explicit phrase and bounded spelling expansion; compare with English PostgreSQL full-text and title trigrams |
| B04-030 — Vector Embeddings | Exact cosine/Euclidean baselines, a real bounded HNSW implementation, native pgvector plans, model/source revisions and repeated recall/work/memory measurements |

## An ordered key cannot answer every range efficiently

Sorting points by longitude makes a longitude interval contiguous but leaves latitude filtering within that interval. Sorting latitude first reverses the problem. The KD tree partitions alternate dimensions and rejects subtrees whose boxes do not overlap the query. Candidate and visited-node counts expose the actual pruning work. The spherical radius test needs a second distance check: inclusion in a bounding rectangle alone is insufficient. Antimeridian queries split into two longitude intervals, and candidate identities are deduplicated.

The application uses PostgreSQL's point/box operators and GiST index rather than copying the KD tree into its authoritative database. The fixture verifies outcomes; it does not force a spatial plan or claim that a tiny fixture must make the optimizer choose GiST. Polygon geometry, ellipsoidal distance and spatial joins remain broader work.

## Positions, ranking and spelling have separate jobs

Term postings avoid reading every document. Positions distinguish a phrase from mere co-occurrence; a deliberate field gap prevents a title-ending word and a body-starting word from becoming a phrase. Title weighting changes ranking without changing membership. Document-frequency and length statistics belong to the tenant, so inserting another tenant's documents cannot change this tenant's lab scores.

Edit-distance expansion is explicit and bounded because vocabulary scans can become expensive. PostgreSQL title trigrams provide a different spelling-suggestion contract. Neither mechanism is a trained semantic encoder. The English application parser also stems/removes stop words, while the lab tokenizer does not; compare the behavior and its documented rules, rather than expecting equal numeric scores.

## Approximation needs an exact quality baseline

Run the same query vectors with exact scans and HNSW, intersect returned IDs and divide by the exact result count. Inspect every query as well as the mean: a good average can hide a query with no correct neighbors. Increase `ef_search` while holding the graph fixed, then compare graphs with different `M`. Larger graphs consume more edge storage and construction work; higher search budgets inspect more distances.

The seeded Go implementation makes graph structure/reopening reproducible. Native pgvector has a different implementation, float32 representation and mixed-tenant graph. Its benchmark retains actual forced HNSW plans and strict-order iterative scanning configuration. Exact public API ordering is deliberately kept separate from those diagnostic ANN plans. Recall can remain imperfect under filtering or a small budget.

The hand-assigned “mouse” coordinates make a lexical/topic retrieval contrast inspectable without downloading a model. A production encoder would require its own version, evaluation corpus, quality checks and migration/re-embedding policy. A matching vector dimension alone is not enough to make different model versions comparable.

## Derived data must follow source revisions

Publish document version 1 and its embedding, then update the document to version 2. The old vector must disappear from discovery before replacement generation finishes. A late version-1 publication conflicts; a version-2 replacement with the correct embedding revision restores the hit. Deleting the document excludes the replacement, while both source and embedding history remain immutable.

This is a source-version fence within one PostgreSQL context. It is a useful foundation for a later asynchronous ingestion pipeline; it does not yet implement automatic cross-context projection, a backfill rollout or production model serving.
