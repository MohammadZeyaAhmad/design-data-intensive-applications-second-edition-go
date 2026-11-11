namespace go thriftv2
struct Line { 1: required string sku, 2: required i32 quantity }
struct Snapshot {
  1: required string tenant_id,
  2: required string order_id,
  3: string display_name,
  4: required list<Line> lines,
  // Field ID 5 is retired permanently; Thrift has no reserved declaration.
  6: optional string currency = "USD",
  7: required i64 version,
  8: optional bool expedited,
  9: optional i64 discount_minor,
  10: optional map<string,string> labels,
}
service SnapshotGateway { Snapshot relay(1: Snapshot record) }
