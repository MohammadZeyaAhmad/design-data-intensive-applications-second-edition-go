# GraphQL reads, cursors and stale projections

Run `make verify ID=C03-05` against the local `atlas_test` binding for race/architecture checks, Go comparison and four PostgreSQL tests. The normal integration harness owns a random disposable database and drops only that database. An unavailable binding is a failure. Run `make verify-graphql-live` only against the intended development API/worker profile; it creates uniquely named Catalog/order fixtures through real authenticated HTTP.

`make configure` preserves existing settings and appends a random `ATLAS_GRAPHQL_CURSOR_KEY`. Compose passes it to the API. Keep `.env` owner-only and retain the key through restarts. An absent/short key fails API initialization. Rotating it deliberately invalidates existing cursors; clients must start a new first page. There is no overlapping-key rotation or cursor confidentiality guarantee.

Inspect GraphQL `errors` even for HTTP 200. `STALE_PROJECTION` means an existing view has not reached the requested version. A missing view is null. Check the appropriate worker/broker receipt/quarantine state, and use the existing scoped projection-rebuild workflow when justified. Do not bypass command checks or substitute a current Catalog price for the accepted order price.

`QUERY_LIMIT` is a rejected operation, not a partial successful query. Narrow the selected collections, aliases, page size or text fields. Fragments and false directives still consume a conservative cost budget. Changing the schema or source cardinalities requires updating and testing the cost traversal and string weights, not merely raising a threshold.

`FORBIDDEN` can identify an owner mismatch or a server field-policy denial. Check the authenticated tenant and published contract identity. Do not add a client-supplied tenant argument or share request caches between tenants. `UNAVAILABLE` masks source failures; diagnose server/database state without exposing raw SQL/credentials to clients. Some integrity/prefetch failures null the composed root, while nullable field-policy failures can retain allowed siblings.

Statement counts show batching, not query plan quality or network speed. Inspect scale, row counts and database execution separately before production tuning. Cross-context snapshots, production role assignment, rolling schema/key rotation, rate limits and wider operational SLOs remain planned work.
