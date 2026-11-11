namespace go thriftv1
struct Line { 1: required string sku, 2: required i32 quantity }
struct Snapshot {
  1: required string tenant_id,
  2: required string order_id,
  3: string customer_name,
  4: required list<Line> lines,
  5: optional string legacy_note,
  7: required i64 version,
  8: optional bool expedited,
}
service SnapshotGateway { Snapshot relay(1: Snapshot record) }
