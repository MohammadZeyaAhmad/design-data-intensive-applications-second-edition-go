# Property graphs, Cypher and recursive SQL

This lab represents warehouse routes and supplier relationships as a directed property multigraph in three actual backends: Go adjacency maps, PostgreSQL vertex/edge tables, and Neo4j Community queried through its official Go driver. The same fixture and independent reference calculations define reachability, simple paths and cheapest routes. These are synthetic modeling experiments; they do not authorize real inventory, sourcing or shipment decisions.

```sh
make verify-graph
make verify ID=C03-03
go run ./cmd/guide verify-graph
```

Run from the repository root with Go 1.27+, Docker Compose and Make. These alternatives run Go race tests and a Go report, start the independent graph profile, execute three native integration tests and the full comparison, then stop its containers. Reports are `artifacts/graph.json` and `artifacts/graph-services.json`. CI retains both and runs the full pinned images.

For an existing running graph profile:

```sh
make up-graph
go run ./cmd/guide verify-graph-services
make down-graph
```

The profile uses loopback **17433** for PostgreSQL, **17687** for Bolt and **17474** for Neo4j HTTP. It has separate `ddia-go-graph` volumes and does not require stopping the application/Kafka/Temporal profiles. `make configure` creates the missing graph password in ignored owner-only `.env`, preserving existing credentials. Health checks execute authenticated SQL/Cypher with credentials in child environments. Stopping retains volumes. Keep the matching `.env`; changing an initialization password does not reset an already initialized Neo4j database.

Go-only experiments require no services:

```sh
make graph-test graph-demo
cd labs
go run ./cmd/lab graph -rows 128 -runs 3 -output ../artifacts/graph-small.json
```

Graph rows mean vertices, accept even counts **32–512**, and default to 512. Repetitions accept 1–5. Graph commands keep their Go data in memory and do not use `-workdir` for a persistent store. Other lab commands retain their own row/filesystem contracts.

## Identity, labels and properties

A node has a tenant-qualified ID, kind `warehouse` or `supplier`, and scalar properties. Neo4j stores actual `Warehouse`/`Supplier` labels plus the common `AtlasGraphVertex` label. Edges have distinct tenant-qualified IDs, source/target, kind `route` or `supplies`, and properties. Parallel relationships remain distinct even when their endpoints match. Route endpoints must be warehouses; a supplies edge starts at a supplier and ends at a warehouse.

Properties permit non-NUL UTF-8 strings of at most 256 bytes, int64 values of magnitude at most one trillion and booleans, with at most 16 keys. The JSON codec allows 32 KiB to accommodate escaped forms of valid scalar data. Identity fields cannot be overwritten through a property map. Each node requires a nonempty name. Each edge requires positive integer `cost`, nonnegative `capacity` and Boolean `enabled`. Floating-point/nested/list/null properties are outside this bounded model, even though Neo4j supports additional property forms.

The default fixture has **512 vertices and 522 edges**, split between `acme` and `globex`, deliberately reusing logical IDs across tenants. It includes supplier/warehouse labels, capacity and enabled predicates, parallel routes, self-loops, a directed cycle, diamonds, a disabled shortcut and an isolated warehouse. Different tenant names/topology make accidental cross-tenant matches observable.

## Three explicit query contracts

`Backend.Query(ctx, mode, query)` accepts tenant, start, optional target, edge kind, direction, hop bound and minimum capacity. Kind defaults to `all`, direction to `out`. Eligibility always requires an enabled edge meeting capacity; `in` traverses incoming relationships without changing their stored direction. Tenant is an explicit lab argument. This library supplies no bearer authentication or production authorization boundary.

| Mode | Semantics and ordering |
|---|---|
| `reach` | Existing start is included at hop zero; return each eligible reachable vertex once with minimum hop distance, kind and name; order by hops then ID |
| `paths` | Enumerate node-simple paths to the target within the bound; parallel edge IDs distinguish paths; order by hops, node-ID sequence, edge-ID sequence |
| `cheapest` | One minimum-cost node-simple path within the bound; ties use hops, node IDs then edge IDs |

Missing start/target/tenant gives an empty answer. A zero-hop path from an existing vertex to itself has one node, no edges and cost zero. A self-loop or longer cycle is excluded from node-simple paths. Edge kinds filter relationships, not the starting vertex's label.

Path and cheapest queries allow **0–6 hops**. Reach allows **0–511**. In a graph with at most 512 vertices, every reachable vertex has a simple witness of at most 511 edges, so that reach setting computes complete reachability for the validated graph. Full reach in the default fixture returns 252 warehouses and reaches a vertex 250 hops away; it is not limited to a small neighborhood.

Both incoming and outgoing degree are limited to four. A six-hop expansion therefore examines at most 5,460 edge extensions before pruning. All-path output is limited to **256**; a 257th result returns `ErrBudget` with no partial answer. A separate seven-vertex fixture with three parallel edges per step has 729 six-hop paths and verifies this error in all three backends. Cheapest queries still return the optimal bounded path without treating a truncated path list as an exact answer.

## Algorithms and declarative queries

The Go backend builds both outgoing and incoming adjacency lists. BFS computes minimum-hop reachability. DFS enumerates simple paths with a visited-vertex check. A positive-cost priority queue implements bounded Dijkstra; it retains path state because a single best distance per vertex would be insufficient under a hop budget. Inputs are cloned and queries/complete fixture replacement use a local RWMutex.

The independent oracle scans the whole edge relation. It uses bounded relaxation for hop distances and a separate walk enumeration/sort for paths, rather than calling the adjacency/heap implementation. Every backend must match its complete answer, not just its count.

PostgreSQL stores typed identities/relations with JSONB properties and generated cost/capacity/enabled columns. Recursive reach uses `UNION` on `(vertex, depth)` and groups minimum depth; the finite depth bound terminates cycles. Simple paths use `UNION ALL` with vertex/edge arrays and `NOT next=ANY(node_ids)`. Cheapest orders that bounded simple-path relation by cost and deterministic ties. Arrays use explicit `C` collation for parity with ASCII identities in Go/Cypher.

Neo4j executes real parameterized Cypher, including variable-length `ROUTE|SUPPLIES` patterns, incoming traversal, property predicates, list comprehensions, `reduce` for cost and a node-uniqueness predicate for simple paths. Minimum-hop reach aggregates by endpoint; the recorded engine selected a pruning BFS expansion. All-path and cheapest plans selected ordinary variable-length expansion and `Top`. Native cheapest queries enumerate the bounded relation; they are not claimed to implement Go's priority-queue algorithm or a GDS plugin.

With a six-hop search bound, the direct route to `wh-002` costs 35, while a two-edge path costs 14. Under a one-hop bound, only the direct route qualifies. Updating its cost to 3 changes the cheapest answer to that direct edge. “Fewest hops” and “lowest cost” are different questions.

## Tenant fences and fixture publication

Go adjacency keys include tenant. PostgreSQL primary/foreign keys include run, tenant and node identity, so a relationship cannot reference another tenant's endpoint. Every recursive join and result projection preserves the run/tenant predicate.

Neo4j anchors by `(run, tenant, ID)` using a composite uniqueness constraint. Every node **and** relationship in a path must belong to that run/tenant. Filtering just the start and final node is insufficient: a path can leave a tenant and return. Native tests deliberately insert such a bridge and a relationship with the wrong tenant; neither may change the requested tenant's result. The SQL equivalent is rejected by its composite foreign key.

`Replace` validates and publishes a complete fixture. PostgreSQL uses one transaction; Neo4j uses one managed write transaction. Each adapter's RWMutex serializes its own readers against replacement. Concurrency tests require an entire old or new answer. This is a single-client lab guarantee, not cross-client Neo4j snapshot isolation, an online ingestion pipeline or a versioned event store.

Each native invocation creates and drops only a randomly named PostgreSQL test database. Neo4j fixtures use fresh run markers and scoped nodes, and cleanup removes those owned nodes and their attached relationships. It never clears the whole Neo4j database. Source fixtures are bounded to 512 vertices, 2,048 edges and 8 MiB. The Go graph is volatile; native profile volumes persist independently.

## Plans, measurements and remaining work

The Go report contains **144 raw samples**: 16 cases × three phases × three repetitions. The native report contains **432 samples** across three backends, plus nine separate publication timings. The phases are initial data, property replacement and vertex deletion. Every case has a warm run; backend order rotates by case. Serialized fixture bytes are logical payload, not resident memory or device traffic.

The native report retains **15 actual plans**: five PostgreSQL cases before/after outgoing/incoming indexes, and five Neo4j `PROFILE` trees. PostgreSQL statistics are refreshed in both phases; mandatory identity/foreign-key indexes remain. No planner switches or forced index hints are used. Neo4j plans retain operators, rows, DB hits, page-cache counts and operator time. SQL buffer hits and Neo4j DB hits have different meanings and should not be ranked as the same unit.

The profile pins PostgreSQL 18 and Neo4j Community 5.26.33 with driver v5.28.4. Neo4j heap is 256 MiB and page cache 64 MiB; PostgreSQL WAL bounds/checkpoint interval constrain the disposable profile's disk footprint. Publication timings include native transaction/storage work under those settings. Native query times include client round trips. These warm measurements establish no universal engine ranking or production SLO.

See the [chapter walkthrough](chapters/ch03/property-graphs.md), [decision](adr/0014-bounded-property-graphs-and-native-queries.md), [runbook](runbooks/graph-lab.md) and [validation](VALIDATION.md). C03-03 remains in progress for its full sub-concept audit, broader labels/properties, larger/high-degree graphs, concurrent multi-client publication and operational work. The [RDF/SPARQL/Datalog lab](SEMANTIC_GRAPHS.md) now extends the fixture with five-backend membership comparison as separate package C03-04.
