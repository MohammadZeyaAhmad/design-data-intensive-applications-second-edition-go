# ADR 0012: Search discovery with source-versioned embeddings

Status: accepted, 11 November 2026.

Product discovery needs executable text, spatial and vector experiments without making an approximate hit authoritative for price, stock or checkout. Add Search as a private application context with published discovery contracts, and keep direct data-structure implementations in the independent lab module.

Use synchronous bbolt records and version/tombstone floors for the bounded lab. Rebuild tenant-specific inverted/KD/HNSW indexes lazily in deterministic order. This makes mutation/reopen correctness inspectable and leaves online graph deletion and larger scale as explicit future work. Measure exact versus approximate identities, work and payload-memory estimates; do not infer semantic language quality from synthetic vectors.

Use native PostgreSQL English full-text, title trigrams and point/box queries in the application. Install the pinned vector extension/schema through a separate opt-in initialization command, with advisory locking and checksum checks. The default profile remains usable with text/spatial operations. The vector application/test profiles use separate named volumes from the default application.

Fence each embedding by tenant, document, model/version, source document version and expected embedding revision. Hold a shared source-row lock while publishing; atomically persist the embedding, immutable history and idempotency receipt. Only embeddings joined to a current live source revision may be retrieved. A document change therefore invalidates previous embeddings immediately, even if replacement computation is delayed.

Keep the public vector query exact with deterministic ties. Compare native HNSW in an explicitly forced diagnostic benchmark, retaining plans and raw recall/latency samples. Its shared multi-tenant graph and postfilters have different recall properties from the Go lab's separate tenant graphs. Production approximate API policy, automatic ingestion and model rollout need their own designs and evidence. See the [guide](../SEARCH.md).
