# ADR 0016: GraphQL composes published tenant-scoped read contracts

Status: accepted, 11 November 2026.

C03-05 needs an actual alternative read endpoint with batched nested data, without allowing a transport adapter to query another context's private tables or use eventual views as command authority.

Publish bounded projection/product/detail reader contracts. Implement their SQL inside the owning contexts, reusing the generated Catalog detail mapping. Compose them in `api/graphql` with graph-gophers/graphql-go and a versioned SDL. Enforce this dependency boundary in architecture tests. Preserve REST commands and their authoritative validation.

Prefetch selected collection identities and memoize request-local futures, including misses/errors. Bind every cache instance and returned identity to its authenticated tenant. Use the same canonical permission for deprecated/current price names. Use custom decimal-string Long values to retain int64 precision.

Use identity-keyset pages with tenant/resource/version-bound HMAC cursors and a persistent configuration key. Validate query shape/cardinality/string weights before execution with bounded gqlparser preflight; retain full engine validation, parallelism/context limits and explicit error/null semantics. Runtime introspection and write/subscription protocols are disabled for this endpoint.

Compare complete selected answers through actual GraphQL/REST adapters and count real pgx statement executions for 1/10/100 lines. Separate those counts from the Go fixture's contract-call counts, database operator work, payload size and network latency. Test stale projections, current versus frozen pricing, tenant cursors and rejected-query zero-read behavior.

The result is a read composition adapter, not a graph database, global snapshot, GraphQL mutation surface, production identity provider or complete schema rollout system. Larger C03-05, APP-08, C03-06 and P37 obligations remain in the saved plan.
