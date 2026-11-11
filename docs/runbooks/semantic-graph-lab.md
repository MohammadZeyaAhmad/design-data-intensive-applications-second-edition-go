# Semantic graph profile and owned recovery

`make verify-semantic` owns the `ddia-go-rdf` Compose lifecycle and stops its services on exit. `make up-rdf` leaves them running for `go run ./cmd/guide verify-semantic-services`; `make down-rdf` retains its independent PostgreSQL and TDB2 volumes. Default loopback ports are 18433 and 18303. The application and Neo4j profile can retain their own state independently.

`make configure` preserves credentials and creates the RDF password only when missing. PostgreSQL initialization does not reset an existing volume's password. Investigate a mismatch while preserving `.env` and data. Do not disable authentication or delete volumes to make health checks pass. The runtime preparation script verifies the pinned Fuseki JAR before using it; an integrity failure must be investigated, not bypassed by changing the expected hash without a reviewed version update.

Every native invocation owns a fresh `atlas_semantic_run_*` PostgreSQL database under a loopback `atlas_test` administrator. RDF source/scratch graphs use fresh `urn:atlas:semantic-run:<random>:` scopes. Cleanup drops only that owned database and those graph URIs; no `CLEAR ALL` or dataset-wide deletion is used. A query attempts scratch cleanup with an independent bounded context after cancellation. If that fails, the store retains its scope for Close to retry; close failures propagate from the runner.

After an uncertain update response, republish the complete owned fixture or start another owned run. Do not accumulate old Datalog derivations after deleting source facts: reevaluate from current inputs. Do not infer authorization to clean another run from an interrupted test. Preserve the exact run URI/logs before investigating leftover data.

An absent start is empty even for reflexive reach. A `+` result containing its start is valid when a real eligible cycle exists. Unexpected duplicate SPARQL solutions can be normal bag semantics; use the defined DISTINCT membership contract. Tenant predicates must cover the edge and both endpoints before path evaluation. Post-filtering the final endpoint does not prevent traversal through a foreign intermediate node.

Budget or timeout errors are failures, not successful partial closure. Keep the documented vertex/degree/fact/body/work limits unless the execution analysis is updated. Go models are volatile. Native persistence is supplied by PostgreSQL/TDB2; server crash recovery, backup/restore, production identity and multi-client cutover remain unverified operational scope.
