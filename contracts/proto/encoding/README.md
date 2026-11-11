# Versioned Protobuf encoding fixtures

The two schemas describe compatible bounded snapshots while deliberately renaming tag 3, retiring tag/name 5, adding optional scalars/maps and widening line quantity from int32 to int64. Versioned Protobuf packages and descriptor paths let both generated Go versions coexist. Actual deployed services must also coordinate message identity, envelope/schema selection and rollout; this lab supplies versions explicitly.

Tag 3 is stable in binary, while the generated ProtoJSON name changes. Native generated messages retain unknown fields across unmarshalling, edits and marshaling. Converting through a domain struct drops them. Tag/name 5 are reserved to prevent future reuse; generation also runs a negative real-compiler fixture. A native i64 value of 4,294,967,297 reads as int32 1, so the public decoder checks numeric business bounds before narrowing.

`make generate-encoding` uses pinned protoc 36.2 and protoc-gen-go 1.36.6. Generated sources live in `labs/ch05/encoding/generated` so the root application and lab modules each build independently. This does not replace the application's JSON event log or its upcasters.
