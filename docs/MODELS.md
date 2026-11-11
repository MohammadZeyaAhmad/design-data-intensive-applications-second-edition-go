# Catalog data models and query mappings

This increment models products, brands, variants, category memberships and customer favorites using shared relational references, repeated flat rows, embedded documents and partitioned column-family keys. Equivalent questions and independent fixture reductions establish correctness. Deliberately stale copies, schema drift and nested updates make the maintenance costs visible.

The application adds normalized Catalog details with JSONB attributes. Prices and quotes retain their existing authority. Comparison tables and customer data are isolated synthetic fixtures, not a new production Customer context or automatic Search projection.

```sh
make up                       # supplies the normal local atlas_test binding
make verify-models
make verify ID=C03-01
go run ./cmd/guide verify-models
make verify-models-live
```

Run from the repository root with Go 1.27+, Make and a local PostgreSQL 18 test binding. Scoped verification runs the Go lab and four real PostgreSQL tests, saving `artifacts/models.json` and `artifacts/models-postgres.json`. As with other integration tests, it creates a random database under the configured local `atlas_test` administrator and drops only that database afterward. It fails if the test binding is unavailable.

For Go-only experiments:

```sh
make models-test models-demo
cd labs
go run ./cmd/lab models -rows 1024 -runs 3 -output ../artifacts/models-large.json
```

Rows accept 32–4,096 and repetitions 1–5. `-workdir` selects a parent for owned temporary children. The runner does not adopt existing files. The library rejects unrelated bbolt buckets and validates fixture identities, references and expansion limits before publication.

## Relationships and representations

Application routes use the existing tenant-bound bearer credential. Mutations require JSON content type and `Idempotency-Key`:

| Route | Contract |
|---|---|
| `PUT /v1/catalog/brands/{id}` | `name`, `expected_version`; returns the new brand revision |
| `GET /v1/catalog/brands/{id}` | Current tenant-local brand/name/revision |
| `PUT /v1/catalog/products/{sku}/details` | `brand_id`, `description`, `attributes`, `variants`, `categories`, `expected_version` |
| `GET /v1/catalog/products/{sku}/details` | Joined current brand name, ordered variants, sorted categories and detail revision |

For example, after publishing a product price and brand `maker`, submit:

```json
{
  "brand_id": "maker",
  "description": "Wireless pointing device",
  "attributes": {"wireless": true, "weight_grams": 120},
  "variants": [{"code": "blue", "label": "Blue"}],
  "categories": ["devices", "electronics"],
  "expected_version": 0
}
```

Variants allow at most 32 unique codes, labels of 100 UTF-8 bytes, and preserve input order. Categories allow at most 16 unique identifiers and return sorted. Missing attributes default to an empty object; null/arrays are invalid. Description is bounded to 4,096 UTF-8 bytes and brand names to 200. Invalid input returns 400, missing tenant-local source/brand 404, and stale revision or changed-key payload 409. Both revision sequences start at one after expected version zero.

| Relationship or access pattern | Implemented representation |
|---|---|
| Product → brand, many to one | Tenant-qualified brand reference; the application joins the current name |
| Product → variants, one to many | Separate ordered application rows; embedded options in document/column-family fixtures |
| Products ↔ categories, many to many | Unique membership rows; repeated categories/entries in the comparison layouts |
| Customers ↔ favorite products | Tenant-qualified membership rows and a customer JSONB document in the SQL fixture; the Go fixture keeps the same logical memberships |
| Category products below a price | Declarative SQL over each representation; a `(tenant, category, price, SKU)` query table and ordered bbolt keys |

The Go reference model materializes typed facts in memory after opening. Its explicit loops resolve brand/favorite references and serve as a procedural comparison. Embedded documents require decoding a complete JSON value; flat rows repeat names for each category/variant and require grouping to recover cards. All modes return the same ordered SKU/name/brand/price/variant-count projections for category, price and favorite questions, including missing tenants/customers.

The PostgreSQL companion publishes prices, brands and details through real Catalog features. On a dedicated connection it builds temporary flat rows, JSONB product/customer documents and query-shaped family tables from those facts. It compares all four SQL representations with an independent Go reduction. SQL chooses an access strategy from the declared query and indexes; the test does not prescribe a procedural join order.

The report retains eight actual `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans, before/after adding comparison indexes, with current statistics in both phases. Application primary/foreign-key and lookup indexes remain present in both phases. “Before indexes” therefore refers to the temporary comparison tables, not to removing application constraints. There are no planner enable-switch changes, and a small or unselective fixture may legitimately use a scan.

## Wide-column modeling versus columnar analytics

The Go family key is a tenant/category partition, followed by big-endian nonnegative price and SKU clustering components, then an `identity` or `options` qualifier. A prefix seek stays inside one partition, and price ordering lets it stop at the maximum price/result limit. The identity family contains a discovery projection; options remain in a separate family. Tenant identity is part of every partition, not a caller-controlled postfilter.

The SQL version uses an equivalent composite B-tree query table with JSONB family values. Favorites are an explicit membership filter over category results. A query starting from a customer, a brand or a different order would need another query table or additional work. A partition/clustering key is a modeling choice tied to a query, not a general substitute for joins.

These are executable **wide-column modeling exercises** using bbolt/PostgreSQL persistence. They implement no Cassandra/HBase cluster, distributed partition placement, repair protocol or wide-row operational guarantees. Analytical [column storage](COLUMNSTORE.md) instead groups values of a field across many facts, using compression and vectorized scans. The two meanings of “column” have different physical and query goals.

## Updates, copied names and repair

The application stores a brand name once. A brand rename advances its own revision and becomes visible through every joined detail read without rewriting those products. The product detail revision and price revision remain unchanged. A historical command receipt/history record preserves what that earlier command returned; a fresh read joins current references.

The Go anomaly exercise updates the normalized brand and just one copied document name, leaving other documents, flat rows and family projections stale. The SQL exercise renames the actual shared brand while its temporary copies remain stale. Result comparison detects both cases. Explicit repair rebuilds all copies from current facts and must restore equality. This demonstrates a maintenance obligation; production Catalog does not serve these deliberately inconsistent comparison copies.

Go repair/deletion publishes a complete snapshot and all family keys in one synchronous bbolt transaction. Reopening must preserve the repaired/deleted result set. The source fixture is limited to 4 MiB, 4,096 products, 128 brands/customers, 32 variants and 16 categories per product, and 32,768 expanded flat rows. Reopen bounds snapshots to 64 MiB and family cells to 65,536. Writers/queries serialize through one mutex. The durable snapshot is a lab publication mechanism, not a new relational storage engine or a crash-certified database.

## Schema flexibility and nested locality

Application attributes accept a bounded JSON object with up to 32 top-level keys and 4,096 input bytes. Different products may carry different attributes. Existing Go/domain rules still constrain identifiers, relationships, sizes and typed fields; JSONB does not remove the need for a contract.

The schema-on-read exercise supports explicit v1 string weights and v2 integer weights. It migrates the approved old form, distinguishes absent/null from zero, and rejects unknown versions, negative/fractional/overflowing values and the wrong type in v2. Native PostgreSQL JSONB accepts both string and numeric fields; decoding through the current typed Go contract exposes that drift. Schema flexibility shifts validation/migration work rather than eliminating it.

The Go report contrasts the serialized complete nested document with one changed variant row; those are logical representation bytes. The native experiment adds 64 KiB of deterministic hash-string padding to a document and performs a real `jsonb_set` of its first variant label, compared with updating one copied variant row. Actual plans include buffer/WAL counters, and total relation allocation is measured before/after. Both update targets are temporary and **unlogged**, so WAL is zero by design. Verification checks both new values, preserved padding and unchanged authoritative source rows. Allocation/buffer effects do not measure device traffic or predict every TOAST/index/cache configuration.

## Explicit mapping and a real ORM

The application read is generated by **sqlc 1.30.0** from reviewed SQL, including tenant-qualified joins, ordered variants and category arrays. The benchmark compares that mapping with actual **GORM 1.31.1** associations through its PostgreSQL driver. Composite tenant keys are included in every association.

For 12 products the measured statement counts are **1** for the sqlc join, **37** for lazy loading three associations per product, and **4** for eager `Preload`. All return the same full-detail digest. The SQLc wrapper and GORM logger count actual read statements; the logger does not print SQL values. ORM auto-migration is not used. GORM belongs to the comparison package and is not imported by application domain/features or the API binary.

```sh
make generate-catalog
make check-catalog-generated
```

The helper uses sqlc v1.30.0 from `PATH`, or downloads the Linux amd64 release asset and verifies its pinned SHA-256 before running it. Other platforms need that version on `PATH`. CI regenerates and rejects checked-in mapping drift. Ordinary builds use the checked-in generated Go and need no sqlc executable.

## Evidence and remaining work

Default reports use 512 products, two tenants, four questions, four modes and three repetitions: **48 query samples per report**. Mapping counts are a separate three-adapter experiment, not three timed repetitions. Raw warm timings, logical work, actual SQL plans and nested-update evidence have different accounting boundaries and establish no universal model ranking.

Tests add 160 seeded independent questions, duplicate SKUs across tenants, ordering/limits, missing references, concurrent query/repair, partial rename, deletion/reopen, schema migration fuzzing and four real PostgreSQL/application tests. The live guide verifies actual HTTP detail publication/replay, shared-reference rename, expected-version conflict, child replacement, tenant isolation and independent price versions.

See the [chapter walkthrough](chapters/ch03/catalog-models.md), [decision](adr/0013-catalog-models-and-explicit-mappings.md), [runbook](runbooks/catalog-details.md) and [validation](VALIDATION.md). C03-01 remains in progress for its complete sub-concept audit, larger scale, schema rollout/backfill and broader customer/application operations.
