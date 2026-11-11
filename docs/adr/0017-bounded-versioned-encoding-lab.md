# ADR 0017: compare versioned encodings behind bounded laboratory contracts

Status: accepted, 12 November 2026.

C05-01 needs practical encoding/evolution evidence without migrating authoritative event histories as a side effect of a learning experiment. Keep one finite synthetic order-snapshot domain in the independent Go lab module. Publish canonical JSON Schema, Protobuf and Thrift IDLs in their planned repository paths, with reproducible generated/embedded copies owned by that module.

Run seven actual codecs, old/new writer-reader pairs, both relay directions and generated Thrift client/processor combinations. Check common-field equality separately from version-metadata preservation. Native generated Protobuf and raw-object JSON relays demonstrate a preservation path beside lossy typed projection. Use distinct Protobuf descriptor paths/packages so both schema generations coexist. Retire field 5 permanently; let protoc reject reuse and record Thrift's separate review-only history.

Separate native parsing from valid business values. Enforce finite bytes/counts/depth before high-level allocations and integer narrowing. Record actual Thrift Go defaults and required-type behavior without assuming equivalence to Protobuf or other generators. Decode gob only in a concrete-type Linux process with kernel/soft memory limits, CPU and wall budgets, bounded IPC and owned cleanup. This is resource isolation, without a general sandbox claim.

Retain repeated bytes, wall time, actual process CPU, parent allocations and complete semantic digests. State child-process/setup costs, schema-validation differences and self-contained versus stream framing. No universal format ranking follows from these samples. Keep production event stores, upcasters and context boundaries intact; defer writer-schema registry resolution to C05-02 and real deployment coordination to its planned work packages.
