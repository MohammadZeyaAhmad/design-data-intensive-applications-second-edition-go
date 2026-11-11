# Chapter 3: Graph-like data models and queries

`make verify ID=C03-03` runs the [graph guide](../../GRAPHS.md) over warehouse routes and supplier relationships. The original book inventory remains the final coverage gate.

| Source section | Executable concept exercise |
|---|---|
| B03-016 — Graph-Like Data Models | Connected warehouses/suppliers need cycles, parallel links and incoming/outgoing traversal beyond a single nested tree; Go, SQL and Neo4j agree |
| B03-017 — Property Graphs | Nodes/relationship identities, actual labels/types, scalar properties, direction, multigraph paths and property replacement/deletion |
| B03-018 — The Cypher Query Language | Real variable-length patterns, property/tenant predicates, list projections, cost reduction, node-simple paths, deterministic ordering and native `PROFILE` |
| B03-019 — Graph Queries in SQL | Real vertex/edge schema, tenant-qualified foreign keys, recursive `UNION` reach, `UNION ALL` path arrays, cycle guards and before/after adjacency plans |

## A relationship is more than repeated text

A supplier can reach a warehouse through a supplies link followed by routes. Warehouse routes can branch and cycle, and two distinct links can share endpoints with different costs/capacities. Keeping edge identity and properties makes those differences explicit. A tree-shaped document can represent some neighborhoods, but shared/cyclic relationships require references, duplication or another structure.

The lab uses two semantic node labels and relationship types. Neo4j stores actual labels/types, while SQL uses constrained discriminator columns and foreign keys. The explicit Go adjacency representation chooses access structures directly. These are equivalent modeling choices for the fixture; they are not claims that every graph model/database feature is implemented.

## Ask the intended path question

Minimum hops, all node-simple paths and minimum cost answer different questions. A zero-hop start is a real result. Parallel relationships yield different paths even when their vertex sequences match. Positive costs justify the bounded Dijkstra search; unsupported nonpositive weights are rejected rather than silently changing its assumptions.

Use a finite six-hop bound for enumeration and reject excessive results. For reachability, 511 hops covers every possible simple witness in the maximum 512-vertex fixture. The long-cycle case reaches well beyond six hops. The SQL depth-state union terminates because the state space is bounded; changing to unbounded `(vertex, depth)` recursion would lose that termination argument. An unbounded membership-only closure can instead deduplicate vertex identity alone, but would not directly retain minimum depth in the same state.

## Declarative syntax leaves room for execution choices

The Cypher reach query describes endpoints and eligible paths, while the observed optimizer chooses pruning BFS. Its cheapest query enumerates eligible simple paths and sorts cost; the Go backend explicitly uses a priority queue. Both are correct under the same contract without executing the same algorithm.

Likewise, a recursive SQL query can use scans or adjacency indexes depending on frontier size and statistics. Adding an index does not promise its selection or a fixed speedup. Inspect the retained plans and their actual work. A database buffer access, a Neo4j store hit and a Go map lookup are not interchangeable physical I/O counts.

## Tenant predicates belong at every hop

An end-only predicate permits an acme → globex → acme route. The native corruption test constructs that bridge and a wrong-tenant relationship directly in Neo4j. Per-node and per-edge predicates must exclude both. SQL instead prevents the cross-tenant endpoint through composite foreign keys and preserves those keys in recursive joins. These are modeling/query fences; authenticated production access is a separate responsibility.
