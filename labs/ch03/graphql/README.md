# GraphQL read composition exercise

Run from the repository root:

```sh
make graphql-test graphql-demo
make verify ID=C03-05
```

The comparison executes [order-composition.graphql](order-composition.graphql) through the actual production GraphQL adapter, using 1/10/100-line fixtures. The Go report counts published reader calls; the PostgreSQL report counts actual SQL executions and compares complete answers with the existing REST adapters. The native command requires the local `atlas_test` binding. API code and the comparison runner live in the application module, preserving the separate lab module's standalone build and the rule that production never imports labs.

Try [order-page.graphql](order-page.graphql) against the authenticated development endpoint. Change aliases/fragments, page size and selected fields; test missing/foreign identities, a stale minimum version and a cursor from another tenant. Compare current product prices with the order's frozen accepted quote values.

Read the [guide](../../../docs/GRAPHQL.md) for exact limits, null/error behavior, ownership, snapshot boundaries and schema evolution. GraphQL's API selection graph is independent of the [property-graph](../property-graph) and [semantic-graph](../semantic-graph) storage/query models.
