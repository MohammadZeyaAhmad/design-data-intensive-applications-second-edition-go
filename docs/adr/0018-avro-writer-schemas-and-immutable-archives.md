# ADR 0018: retain full Avro writer contracts independently of current code

Status: accepted, 13 November 2026.

C05-02 must distinguish actual historical writer bytes from the current reader's expectations. Use pinned `github.com/twmb/avro v1.9.1` for real encoding, resolution and OCF I/O. Keep three immutable authored schemas with reproducibly generated ordinary structs in the independent lab module, alongside dynamic map/schema construction. Use the application's existing compression dependency version in the lab too.

Identify complete contracts by normalized full-schema SHA-256, preserving defaults, aliases and field order. Record Parsing Canonical Form hashes and CRC-64-AVRO separately. A CRC lookup naming multiple full schemas fails explicitly; pinned consumers may supply the complete writer identity and verify the standard header too. Missing writer schemas never fall back to a latest layout. The bounded registry is append-only and concurrent reads do not expose mutable schema bytes.

Archive actual standard OCF files with embedded full writer schemas and semantic identity metadata. Publish content-addressed files and immutable named manifests with synced temporary inodes and atomic create-if-absent linking. Reopen with only the selected reader registered; use the embedded historical writer. Validate complete files, counts, digests and domain records before returning any results. A publication conflict can leave an unreferenced content object, without changing the winning dataset.

Apply finite schema/wire/count/decompression bounds before native allocation. Support explicit negative Avro array/map blocks and reject duplicate maps, unsupported recursion/logical forms and out-of-profile codecs. Record the runtime's conservative whole-writer-union checks and exact reader-branch preference; include those choices in future cross-language audits.

Retain repeated raw-byte/wall/allocation samples separately from OCF publication and compression. Captured catalog metadata demonstrates runtime schema creation without claiming a live exporter or registry server. Production event migration, rolling deployment, object-store retention, power-loss certification and external schema governance remain their own planned work.
