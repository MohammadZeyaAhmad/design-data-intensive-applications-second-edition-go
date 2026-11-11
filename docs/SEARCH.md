# Search and product discovery

Search connects the Chapter 4 multidimensional, full-text and vector indexing concepts to product and warehouse discovery. A Go lab implements the structures directly. A seventh application context supplies authenticated PostgreSQL search, command receipts, immutable history and an optional pgvector profile. Search hits carry SKU/warehouse references; Catalog prices, Inventory availability and checkout decisions retain their existing authorities.

```sh
make verify-search
make verify ID=C04-05
go run ./cmd/guide verify-search
```

Run these alternatives from the repository root with Go 1.27+, Docker Compose and Make. They run the lab race tests and report, then start an isolated pinned pgvector database on loopback port **16433**, execute six real PostgreSQL/search tests and stop that test service. `make configure` preserves or creates ignored owner-only `.env` credentials. The profile has its own retained database volume; each test invocation creates and drops only its randomly named test database. Reports are `artifacts/search.json` and `artifacts/search-pgvector.json`.

The Go-only experiment needs no services:

```sh
make search-test search-demo
cd labs
go run ./cmd/lab search -rows 1024 -runs 3 -output ../artifacts/search-large.json
```

Rows are bounded to 32–4,096 and repetitions to 1–5. `-workdir` selects a parent filesystem for owned temporary children; the runner never adopts an existing database.

## Durable records and derived indexes

The lab stores tenant-qualified documents, strictly increasing versions, tombstone floors and model configuration in synchronous bbolt transactions. Text postings, a KD tree and an HNSW graph rebuild lazily from current live records after a change or reopen. Rebuild order and graph seed are deterministic. Updates invalidate the relevant tenant's derived structures; deletion cannot be undone by resubmitting an older version. This favors transparent correctness over a production online graph maintenance algorithm.

The lab allows 4,096 retained identities, including tombstones, and 32,768 live vocabulary terms per tenant. Titles/body are bounded to 256/4,096 UTF-8 bytes, tokens to 64 runes and document token count to 1,024. One persisted configuration selects a model/version, 2–64 dimensions, cosine or Euclidean distance, graph degree `M` of 2–32, construction budget and seed. Values must be finite and bounded by ±1,000,000; cosine vectors must be nonzero. Configuration mismatch fails reopening. Different graph configurations in the comparison use separate temporary stores.

All public lab operations serialize through a mutex. Caller vectors/points are copied. Missing-tenant queries do not create retained tenant caches. bbolt provides file locking and durable transaction recovery; this lab does not add another WAL/page engine.

## Three different query structures

| Query | Go implementation | Application implementation |
|---|---|---|
| Text | Inverted term/position postings, tenant-local BM25-style ranking and title weighting | Weighted English `tsvector`, GIN and `websearch_to_tsquery`; `ts_rank_cd` |
| Misspelling | Explicit bounded edit-distance expansion | Explicit title trigram similarity with GIN and transaction-local threshold 0.25 |
| Rectangle | Median-split two-dimensional KD tree with subtree bounding boxes | PostgreSQL point/box containment and GiST |
| Radius | KD bounding candidates followed by exact spherical Haversine distance | Bounding candidates followed by the same spherical distance formula in SQL |
| Exact neighbors | Full scan with cosine/Euclidean distance and deterministic ID ties | pgvector exact distance sort and ID tie break |
| Approximate neighbors | Seeded multilayer HNSW with greedy descent, bounded layer search and diversity pruning | Native pgvector HNSW in the explicit benchmark profile |

The lab tokenizer lowercases Unicode letters/numbers and retains combining marks inside tokens. It performs no stemming, accent folding or Unicode normalization. `All`, `Any` and exact positional `Phrase` modes are distinct. A phrase cannot span title/body. Queries are bounded to 256 bytes and 16 tokens. Fuzzy distance is 0–2, at most four alternatives per term; fuzzy queries have at most four tokens and cannot also request phrase matching. These rules differ deliberately from PostgreSQL's English stemming, stop words and web query syntax; the two engines need not produce identical ranks.

Coordinates use longitude/latitude degrees. Rectangle edges are inclusive. `min_lon > max_lon` means a rectangle crosses the antimeridian and becomes two intervals. Radius search first finds a conservative spherical bounding box, including pole/global cases, then filters exact distances using mean Earth radius 6,371.0088 km. This is a spherical point model, without ellipsoid, polygon or PostGIS operations.

HNSW stores nodes at deterministic randomly selected levels, bounded neighbor lists and a top entry point. Queries descend greedily through upper levels and explore level zero with candidate/result heaps controlled by `ef_search`. Approximate queries do not fall back to a full scan. `M`, construction effort and query effort change build cost, graph size, distance work and recall. The exact scan remains the correctness/quality baseline.

The contrast fixture searches for “mouse”: lexical matching finds the animal and computer device, while manually assigned topic vectors retrieve the device and a peripheral. Those coordinates illustrate retrieval behavior; they do not demonstrate natural-language understanding or a trained embedding model.

## Application context and API

`internal/search/domain` holds validation, `features/publish` and `features/embed` are command slices, `ports` defines persistence, and HTTP/PostgreSQL are adapters. `contracts/search` publishes discovery hits and a text-reader interface. The architecture test enforces the same private-context boundaries as the other six contexts. Document publication is curated and explicit; automatic Catalog event/CDC ingestion remains future work.

Every route requires the existing tenant-bound bearer credential. Document and embedding mutations also require `Idempotency-Key` and JSON content type. The server records its actor/correlation/cause/request envelope in immutable history. A concurrent duplicate returns the saved response; a changed payload under the same key conflicts. Expected versions are compare-and-set preconditions, with zero for first publication. Successful publication advances the version by one.

| Method and route | Input or result |
|---|---|
| `PUT /v1/search/documents/{id}` | `sku`, `warehouse`, `title`, `body`, optional `point:{lon,lat}`, `expected_version` |
| `GET /v1/search/documents/{id}` | Current live document; missing/deleted identities return 404 |
| `DELETE /v1/search/documents/{id}` | JSON `expected_version`; retains a versioned tombstone |
| `GET /v1/search/text?q=wireless+mouse` | English full-text hits |
| `GET /v1/search/suggest?q=wireles+mouse` | Explicit title trigram suggestions |
| `GET /v1/search/box?min_lon=170&min_lat=-10&max_lon=-170&max_lat=10` | Wrapped rectangle hits |
| `GET /v1/search/near?lon=179.9&lat=0&km=20` | Hits within a spherical radius, ordered by distance then ID |
| `PUT /v1/search/documents/{id}/embedding` | Source `document_version`, `model`, `model_version`, `dimensions:8`, `values`, `expected_version` |
| `POST /v1/search/vector` | `model`, `model_version`, eight `values`, optional `metric` and `limit`; exact neighbors |

GET query limits default to 10 and accept 1–100; vector requests use the same default/bounds. Vector metric defaults to `cosine`, with `euclidean` also supported. Hit fields include document version and, for vector hits, embedding revision. Empty result sets currently serialize as JSON `null`. Invalid input returns 400, version/request conflict 409 and disabled vector operations 503 with `vector_profile_required`.

Application embeddings are fixed **eight-dimensional float32** pgvector values. Inputs must remain nonzero after float32 conversion for cosine-index compatibility, even when requesting Euclidean distance. Model and model version are separate bounded identifiers. Each `(tenant, document, model, model_version)` has its own expected embedding revision, while `document_version` identifies the exact source text revision. Document publication takes an exclusive row lock; embedding publication holds a shared source lock and checks both revisions. Concurrent first-embedding writes are fenced by an expected-revision upsert. Receipts and immutable history commit with the change.

Updating a document immediately excludes its previous embedding through the source-version join. Publishing a replacement against the current source revision restores visibility. Deletion excludes all associated embeddings. Old idempotency receipts remain historical outcomes: replaying an earlier successful embedding request does not make its stale embedding current again.

## Optional pgvector application profile

Stop whichever Atlas profile currently owns ports 15433/18081 before starting this independent project:

```sh
make down                    # or down-kafka / down-temporal for that active profile
make up-search
ATLAS_SEARCH_VECTOR_ENABLED=true make verify-search-live
make down-search
```

This profile uses its own `ddia-go-search` PostgreSQL volume and the pinned pgvector 0.8.1 / PostgreSQL 18 image. The normal migration installs core search and `pg_trgm`. A separate `search-vector-init` command installs vector 0.8.1, vector tables, immutable embedding history and cosine/L2 HNSW indexes. Initialization is advisory-lock serialized and checksum recorded; vector-enabled API startup checks the extension/schema marker. The default application profile supports text/spatial search with vectors disabled. `make verify-search-live` checks its text/spatial and tenant/revision behavior; setting the flag adds embedding invalidation/refresh/deletion checks against the enabled profile.

The public vector endpoint deliberately uses an **exact** sort. The native HNSW benchmark explicitly disables sequential/bitmap scans and sorting within its transaction to select each HNSW operator index. It retains actual `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans and verifies the expected index. These are forced diagnostic plans, not claims about ordinary optimizer choices.

Application HNSW indexes contain multiple tenants/models and apply SQL eligibility filters, with `hnsw.iterative_scan='strict_order'` in the benchmark. Mixed-tenant graph traversal can affect recall even though returned records must belong to the requested tenant/model/current document revision. The Go lab instead builds physically separate tenant graphs. Neither arrangement guarantees exact recall.

## Reports and verification boundaries

The default Go report has 512 documents split between two tenants, six graph builds (two metrics × `M=4/8/16`), `ef_search=8/32/128`, 16 queries and three repetitions: **864 raw recall/latency samples**. Exact `k=8` neighbor identities define recall. It also checks text modes, misspellings, independent spatial scan results, update/delete exclusion and deterministic reopen. Graph build time is separate from warm query measurements. Graph memory estimates count float64 vector payloads and string edge-reference headers, excluding map/object/heap overhead; they are lower bounds, not whole-process memory.

The real pgvector report uses 512 application-published documents/embeddings, two tenants, two metrics, three search budgets, 12 queries and three repetitions: **216 samples and six actual plans**. Native tests also cover embedding revision races, model versions, idempotency, stale-source exclusion and deletes. Core integration tests exercise real HTTP authentication, provenance, invalid coordinates and the disabled-vector boundary.

Go search tests include independent full-DP edit distance, brute-force spatial comparisons, exact-neighbor recall, precommit rollback, unknown postcommit outcomes, reopen and numeric/tokenizer fuzzing. The search increment has no native process-kill or physical power-loss certification; its transaction tests and existing bbolt contract define the evidence. Timings describe warm process/OS state. The PostgreSQL and Go fixtures have different precision/layout/query overhead; raw times do not establish a universal engine ranking.

See the [chapter experiments](chapters/ch04/search-indexes.md), [recovery runbook](runbooks/search-recovery.md), [decision](adr/0012-search-discovery-and-versioned-embeddings.md) and [recorded validation](VALIDATION.md). C04-05 remains in progress for the full sub-concept audit, production ingestion, model lifecycle, scale and operational work.
