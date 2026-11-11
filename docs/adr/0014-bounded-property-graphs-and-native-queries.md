# ADR 0014: Bounded property graphs with real SQL and Cypher adapters

Status: accepted, 11 November 2026.

Use synthetic warehouse routes and supplier relationships to compare directed property multigraphs across Go adjacency maps, PostgreSQL relations and actual Neo4j/Cypher. Keep graph experimentation in the independent lab module and preserve the existing application authorities/context boundaries.

Define query semantics before comparing engines: minimum-hop reach includes the existing start; all paths are node-simple and preserve parallel edge IDs; cheapest uses positive integer costs and deterministic ties within a hop bound. Reach permits enough hops for complete connectivity in the bounded graph. Enumeration has six-hop/four-degree/256-result bounds with explicit overflow errors.

Execute actual parameterized native queries and retain actual plans. Use composite run/tenant/identity keys and foreign keys in SQL; require every path node/relationship to preserve scope in Cypher. Verify both malicious tenant bridges and ordinary cyclic/disconnected fixtures against an independent edge-scan oracle.

Publish whole fixtures through native transactions and serialize each client's replacements against its reads. This supports inspectable lab comparisons without claiming cross-client snapshot isolation or production ingestion. Isolate PostgreSQL in owned random databases and Neo4j in fresh run scopes. Keep credentials in ignored owner-only configuration/child environments and preserve profile volumes.

The optional profile uses pinned full images with bounded heap/page cache/WAL resources. Report publication and query timing separately; retain SQL and Neo4j work units without treating them as equivalent device I/O or a universal ranking. See the [guide](../GRAPHS.md) for the completed bounds and remaining operational/semantic work.
