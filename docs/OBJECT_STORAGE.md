# Object-backed warehouse snapshots

C06-02 separates immutable data from the pointer that publishes it. This Go lab uses warehouse-availability records, the same tenant/warehouse/SKU setting as the application and earlier storage labs. PostgreSQL Inventory remains authoritative. These snapshots do not authorize reservations or resolve concurrent scarce-stock writes.

## Run

Use Go 1.27.3, Python 3.12 with venv/pip, Make and a Unix host. Run from the repository root:

```sh
make verify ID=C06-02
go run ./cmd/guide verify-object-storage
# Individual components:
make object-storage-test
make object-storage-demo
make integration-object-storage
make object-storage-compare
```

The scoped verifier runs race tests, the local file-backed profile, the actual Moto 5.2.5 S3 HTTP emulator, native compute-process drills and cross-backend result comparison. Moto and its 26 dependencies are pinned with hashes in `tools/object-storage-runtime.lock`; application runtimes have no Python dependency. First setup downloads that tool environment. `ATLAS_OBJECT_RUNTIME` selects an absolute cache path when the default `artifacts/object-storage-runtime` is unsuitable. Selected integrations fail when unavailable; they do not silently skip.

Reports are `artifacts/object-storage/file.json` and `emulator.json`. The binary provenance manifest records revision, dirty state, Go version and SHA256. CI retains the reports and provenance. Temporary data, jobs and emulator processes belong to each run and are removed after compute workers stop.

## Publication protocol

| Object | Mutability and purpose |
| --- | --- |
| `segments/<sha256>.json` | Immutable sorted deltas, including tombstones; full body hash, record count and size are checked |
| `manifests/<sha256>.json` | Immutable generation, parent reference, ordered segment references and cumulative command receipts |
| `head.json` | The sole current-generation visibility point; create-if-absent initially, then compare-and-swap using the store's opaque version |
| `pins/<name>.json` | Immutable named snapshot reference; identical retry succeeds, rebinding an existing name conflicts |

A compute worker reads the current head and complete base generation, validates a bounded set-value command and uploads its immutable segments. It then uploads the immutable manifest and conditionally replaces the head. Readers follow one captured head through its manifest and referenced segments. They return a complete generation or an error, without a partial row set. Uploading an object or listing it does not publish it.

Two writers preparing from one head cannot silently replace the same generation: one CAS succeeds, and the other reports a conflict. A caller may explicitly rebase its set-value intent on a fresh generation. This API does not implicitly retry a stale computed read-modify-write. A command ID reused with changed content conflicts permanently. Identical retries return the original immutable generation, even after later publications or compaction.

Named snapshot creation verifies that its reference belongs to published history. A complete orphan manifest cannot become visible by being pinned before its head CAS succeeds.

The S3 adapter uses the actual AWS SDK for Go v2, SigV4 signing, `If-None-Match: *` and `If-Match` on PUT. ETags are opaque conditional tokens, without assuming MD5 or equating them to S3 VersionIds. Conditional writes have no hidden SDK retries. An ambiguous reply triggers command-receipt reconciliation; an absent acknowledgment does not establish that publication failed. Unit HTTP checks verify condition propagation and a single wire attempt on a 503 response.

The local durable adapter serializes conditional updates across processes with a short filesystem lock. It writes/fsyncs a private object, atomically renames it and syncs the directory before success. The memory adapter supplies deterministic fault schedules. Neither local implementation models a distributed object store. Process kills do not prove host power-loss durability.

## Snapshots, compaction and caches

Segments are replayed in manifest order; later values and tombstones replace earlier values. Compaction publishes a new manifest over the materialized live rows through the same CAS protocol. Old manifests and segments remain available to pinned readers and original command receipts. Compaction reduces current read references; this increment does not claim reclaimed storage.

Each compute reader has a finite LRU cache keyed by immutable content identity. Bodies enter it only after SHA256 verification, and schema checks still run when reading them. Returned row values and cached bytes do not alias caller-owned mutable buffers. The current head is fetched on every current read, so warming a reader does not pin its current-generation pointer accidentally.

The fixture imports 256 rows, applies eight updates/tombstones, and compacts ten references to two. An independent map oracle requires 253 live rows at generation 10; the named original snapshot still has 256 rows. A representative cached read uses **one GET / 204 body bytes**, versus **four GETs / 12,532 bytes** for a cold or newly opened reader. No device-cache, network-latency or cloud pricing claim follows from these small measurements.

## Independent compute and crashes

Each backend runs two real writer processes paused before the head CAS, plus an independent reader with eight observations across their release. Exactly one same-generation writer wins; the other reports a conflict. Explicit retry preserves both updates. Separate native process kills occur after segment upload, manifest upload, just before head CAS and after head CAS but before acknowledgment. Pre-publication kills leave the old view intact and inventoryable orphan objects. Retry reuses the uploaded segment. The post-publication kill reconciles to the committed generation without advancing it again. An independently reopened reader verifies each recovered state.

The final fixture has 259 live rows at generation 16 on both backends. Cross-backend comparison checks logical state hashes as well as counts. The quiescent audit follows all retained history and named snapshots to identify orphan candidates. It never deletes candidates during active writes. Re-publishing the requested intent recovers its effect; unrelated orphan manifests stay retained until a separately fenced maintenance/lifecycle policy exists.

## Real S3 contract target

`make object-storage-s3-cloud` runs the same contract, workload and native compute suite against an existing user-supplied bucket. Provide `ATLAS_OBJECT_S3_REGION`, `ATLAS_OBJECT_S3_BUCKET`, `ATLAS_OBJECT_S3_ACCESS_KEY` and `ATLAS_OBJECT_S3_SECRET_KEY` through the environment; temporary credentials may also supply `ATLAS_OBJECT_S3_SESSION_TOKEN`. `ATLAS_OBJECT_S3_ENDPOINT` optionally selects another S3-compatible endpoint. Custom plaintext HTTP endpoints are limited to loopback. The normal AWS path uses SDK-selected HTTPS endpoints and certificate validation.

Each run allocates a fresh `atlas-object-lab/<random-128-bit-id>/` prefix. GET/PUT/DELETE stay within it; listing verifies returned keys remain within that prefix. The cloud target does not create or delete a bucket. Its role needs object GET/PUT/DELETE under the lab prefix and bucket listing scoped to that prefix. All compute processes stop before owned-prefix cleanup, and the report requires a subsequent empty listing. Cleanup failure fails the integration and identifies the remaining owned prefix. A production publisher should use separate permissions and retention policy rather than this disposable test role.

The shared contract requires exactly one winner among sixteen concurrent creates and sixteen replacements from one version, refusal of stale conditions, non-aliasing reads, and complete listing over 105 objects across pagination. It rejects invalid keys and unconditional writes. The emulator exercises actual signed HTTP requests but does not establish AWS durability, IAM enforcement, regional availability, encryption/key recovery or crash persistence of the emulator service. No external AWS bucket was used for the recorded validation.

Logical port counters include attempted/rejected operations. Separate S3 transport counters include actual parent-process HTTP requests, pagination, setup/cleanup, conflicts and body bytes; child-process traffic is excluded from those parent counters. A synthetic provider price is not baked into the report. Cloud request/storage/egress pricing and workload scale must be measured for a chosen deployment.

Bounds are explicit: 128 generations/receipts, 64 segment references, 128 records per segment, 256 changes per command, 4,096 live rows, 1 MiB per object and finite reader memory. Exceeding a limit fails without silently forgetting receipts. Snapshot expiration, safe garbage collection, multipart/large-object formats, distributed caches, cloud IAM/retention/encryption, multi-region operation and full semantic/scale audits remain open.

See the [chapter mapping](chapters/ch06/object-backed-storage.md), [decision](adr/0025-immutable-objects-and-conditional-generation-publication.md), [runbook](runbooks/object-storage-publication.md), [increment plan](object-storage-work-plan.md) and [validation](VALIDATION.md). C06-02 remains in progress.
