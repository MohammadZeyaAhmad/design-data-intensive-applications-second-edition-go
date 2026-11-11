# Search publication and recovery

Use `make verify-search` for owned isolated fixtures. It preserves configured credentials and named profile volumes, creates random test databases and drops only those databases. Reports contain synthetic documents/vectors and timings. `make search-test search-demo` runs only the Go lab.

The lab's bbolt source records, persisted configuration and version floors are durable; derived indexes are disposable. A failed `document.ready` callback aborts publication. A commit I/O error or `document.committed` callback error returns `ErrRecovery` and poisons the handle. Close and reopen the same file, then inspect current query results/source versions before submitting a newer version. The operation may have committed despite its error. Exact duplicate versions are rejected by this ingestion API; it has no command receipt interface.

Opening validates metadata, retained-record counts, model/dimension bounds, source identity and active documents before rebuilding indexes. A mismatched configuration or corrupt source is an error. Do not remove tombstones or change the persisted model to make stale input pass. Rebuilding an index cannot repair corrupt records. The current lab has callback/reopen evidence, not a native search process-kill or physical power-loss certification.

In the application, run normal migrations before `atlas search-vector-init`. The pinned pgvector profile supplies extension files; a default PostgreSQL image does not. Initialization and startup check the expected vector version and schema checksum marker. Investigate a missing extension, checksum mismatch or disabled-profile 503 as a configuration/schema problem; do not edit an applied checksum or copy another profile's database volume.

For a document update, publish the new source revision using its current expected version. Old embeddings then disappear from vector queries. Compute a replacement with the same intended model/version and the new `document_version`, and publish using that model's current embedding revision. A stale source revision or competing embedding writer returns 409. Re-read the source and choose whether recomputation is needed; do not blindly increase only the embedding revision.

After an uncertain HTTP response, retry the identical mutation with the same idempotency key. The saved response identifies that request's outcome; it does not guarantee that a later source update has left its embedding eligible. Fetch current source state and query the intended model/version. Deleted documents stay excluded, and immutable history remains available for investigation.

`ATLAS_SEARCH_VECTOR_ENABLED=true make verify-search-live` checks the enabled application's publish/query/update/stale-conflict/refresh/delete flow. The flag must match the running profile. `make integration-search-vector` checks isolated native HNSW plans and recall; those forced plans are diagnostics and do not promise the production optimizer's chosen access path or a minimum recall guarantee.

Stop only the Atlas profile that owns the shared application ports before switching profiles. `make down-search` and `make down-search-test` retain their volumes. Automatic ingestion, asynchronous backfill, model cutover, source-history retention, backup/restore and production access roles are still tracked work.
