// Package columnstore implements bounded warehouse analytics over immutable
// compressed column segments and a transactional delta. bbolt supplies durable
// publication; the segment format and query engines are implemented here.
package columnstore

import (
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

const (
	GroupRows       = 64
	SegmentRows     = 4096
	MaxIDs          = 65536
	MaxDelta        = 4096
	MaxSegments     = 64
	maxSegmentBytes = 8 << 20
)

var ErrVersion = errors.New("fact version must advance")
var ErrProgram = errors.New("compiled predicate does not match expression")

type Column string

const (
	ID        Column = "id"
	Version   Column = "version"
	Tenant    Column = "tenant"
	Warehouse Column = "warehouse"
	Category  Column = "category"
	Day       Column = "day"
	Units     Column = "units"
	NetCents  Column = "net_cents"
)

var columns = []Column{ID, Version, Tenant, Warehouse, Category, Day, Units, NetCents}

type Value struct {
	Kind  string `json:"kind"`
	Valid bool   `json:"valid"`
	S     string `json:"s,omitempty"`
	I     int64  `json:"i,omitempty"`
}

func number(n int64) Value   { return Value{Kind: "int", Valid: true, I: n} }
func text(s string) Value    { return Value{Kind: "string", Valid: true, S: s} }
func null(kind string) Value { return Value{Kind: kind} }
func kind(c Column) string {
	switch c {
	case Tenant, Warehouse, Category:
		return "string"
	case ID, Version, Day, Units, NetCents:
		return "int"
	}
	return ""
}
func nullable(c Column) bool  { return c == Category || c == Units || c == NetCents }
func validName(s string) bool { return len(s) > 0 && len(s) <= 32 && utf8.ValidString(s) }
func compare(a, b Value) int {
	if a.Kind == "string" {
		if a.S < b.S {
			return -1
		}
		if a.S > b.S {
			return 1
		}
	} else {
		if a.I < b.I {
			return -1
		}
		if a.I > b.I {
			return 1
		}
	}
	return 0
}

// Fact is a synthetic warehouse shipment/adjustment fact. Null measures remain
// distinct from zero, and a null category remains distinct from an empty name.
type Fact struct {
	ID        uint64  `json:"id"`
	Version   uint64  `json:"version"`
	Tenant    string  `json:"tenant"`
	Warehouse string  `json:"warehouse"`
	Category  *string `json:"category"`
	Day       int64   `json:"day"`
	Units     *int64  `json:"units"`
	NetCents  *int64  `json:"net_cents"`
}

func (r Fact) valid() bool {
	return r.ID > 0 && r.ID <= 1e12 && r.Version > 0 && r.Version <= 1e12 && validName(r.Tenant) && validName(r.Warehouse) &&
		(r.Category == nil || len(*r.Category) <= 32 && utf8.ValidString(*r.Category)) && r.Day >= 0 && r.Day <= 36524 &&
		(r.Units == nil || *r.Units >= -1e9 && *r.Units <= 1e9) && (r.NetCents == nil || *r.NetCents >= -1e9 && *r.NetCents <= 1e9)
}
func (r Fact) value(c Column) Value {
	switch c {
	case ID:
		return number(int64(r.ID))
	case Version:
		return number(int64(r.Version))
	case Tenant:
		return text(r.Tenant)
	case Warehouse:
		return text(r.Warehouse)
	case Category:
		if r.Category != nil {
			return text(*r.Category)
		}
	case Day:
		return number(r.Day)
	case Units:
		if r.Units != nil {
			return number(*r.Units)
		}
	case NetCents:
		if r.NetCents != nil {
			return number(*r.NetCents)
		}
	}
	return null(kind(c))
}
func factFrom(values map[Column][]Value, i int) (Fact, error) {
	get := func(c Column) Value { return values[c][i] }
	r := Fact{ID: uint64(get(ID).I), Version: uint64(get(Version).I), Tenant: get(Tenant).S, Warehouse: get(Warehouse).S, Day: get(Day).I}
	if v := get(Category); v.Valid {
		s := v.S
		r.Category = &s
	}
	if v := get(Units); v.Valid {
		n := v.I
		r.Units = &n
	}
	if v := get(NetCents); v.Valid {
		n := v.I
		r.NetCents = &n
	}
	if !r.valid() {
		return Fact{}, kv.ErrCorrupt
	}
	return r, nil
}
func decodeFact(b []byte) (Fact, error) {
	var r Fact
	if json.Unmarshal(b, &r) != nil || !r.valid() {
		return r, kv.ErrCorrupt
	}
	return r, nil
}
func mask(n int) uint64 {
	if n == 64 {
		return ^uint64(0)
	}
	return uint64(1)<<n - 1
}

type Mode string

const (
	Scalar   Mode = "scalar"
	Batch    Mode = "batch"
	Compiled Mode = "compiled"
)

type Query struct {
	Tenant  string
	Filter  *Expression
	Select  []Column
	Mode    Mode
	Program *Program
}
type Projected struct {
	ID     uint64           `json:"id"`
	Values map[Column]Value `json:"values"`
}
type ScanStats struct {
	Groups            uint64 `json:"groups"`
	Pruned            uint64 `json:"pruned_groups"`
	Examined          uint64 `json:"examined_rows"`
	Visible           uint64 `json:"visible_rows"`
	Columns           uint64 `json:"decoded_columns"`
	ColumnBytes       uint64 `json:"compressed_column_bytes_decoded"`
	MetadataBytes     uint64 `json:"segment_metadata_bytes"`
	DeltaBytes        uint64 `json:"delta_json_bytes"`
	VisibilityEntries uint64 `json:"visibility_entries"`
	WallNanos         int64  `json:"wall_nanos"`
	ProcessCPUNanos   *int64 `json:"process_cpu_nanos,omitempty"`
	ChildCPUNanos     int64  `json:"compiled_child_cpu_nanos,omitempty"`
}
type Result struct {
	Tenant       string      `json:"tenant"`
	Rows         []Projected `json:"rows"`
	Stats        ScanStats   `json:"stats"`
	StateVersion uint64      `json:"state_version"`
}
