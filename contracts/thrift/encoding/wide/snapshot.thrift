namespace go thriftwide
// Deliberately incompatible probe, separate from the compatible v2 contract.
struct Line { 1: required string sku, 2: required i64 quantity }
struct Snapshot {
  1: required string tenant_id,
  2: required string order_id,
  3: string display_name,
  4: required list<Line> lines,
  7: required i64 version,
  8: optional bool expedited,
}
