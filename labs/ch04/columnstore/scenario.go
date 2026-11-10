package columnstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/storage"
)

type ProgramEvidence struct {
	Expression string `json:"expression"`
	Source     string `json:"generated_go"`
	SHA256     string `json:"source_sha256"`
	BuildNanos int64  `json:"build_nanos"`
}
type Compression struct {
	Name          string         `json:"name"`
	Rows          int            `json:"rows"`
	EncodedBytes  int            `json:"segment_bytes"`
	MetadataBytes int            `json:"metadata_bytes"`
	ColumnBytes   int            `json:"column_bytes"`
	RowJSONBytes  int            `json:"row_json_bytes"`
	Codecs        map[string]int `json:"column_codecs"`
}
type Measurement struct {
	Phase            string    `json:"phase"`
	Expression       string    `json:"expression"`
	Mode             Mode      `json:"mode"`
	Rows             int       `json:"rows"`
	Digest           string    `json:"result_sha256"`
	AggregateDigest  string    `json:"filtered_group_sha256"`
	StateVersion     uint64    `json:"state_version"`
	Scan             ScanStats `json:"scan"`
	RowJSONScanBytes int       `json:"reference_live_row_json_bytes"`
	ReferenceNanos   int64     `json:"reference_nanos"`
	ReferenceCPU     *int64    `json:"reference_cpu_nanos,omitempty"`
}
type ScenarioRun struct {
	Repetition     int              `json:"repetition"`
	Measurements   []Measurement    `json:"measurements"`
	Phases         map[string]Stats `json:"phases"`
	ReopenVerified bool             `json:"reopen_verified"`
	CubeVerified   bool             `json:"all_tenant_groupings_verified"`
}
type Report struct {
	Passed      bool              `json:"passed"`
	DatasetRows int               `json:"dataset_rows"`
	Repetitions int               `json:"repetitions"`
	Environment map[string]any    `json:"environment"`
	Programs    []ProgramEvidence `json:"programs"`
	Compression []Compression     `json:"compression"`
	Runs        []ScenarioRun     `json:"runs"`
	Definitions []string          `json:"definitions"`
}

func SyntheticFact(i int) Fact {
	c := []string{"tools", "food", "", "returns"}[i%4]
	u, n := int64(i%9-2), int64(i*17%10000)
	r := Fact{ID: uint64(i + 1), Version: 1, Tenant: []string{"acme", "globex"}[i%2], Warehouse: []string{"east", "west"}[(i/2)%2], Category: &c, Day: int64(i / 16), Units: &u, NetCents: &n}
	if i%7 == 0 {
		r.Category = nil
	}
	if i%5 == 0 {
		r.Units = nil
	}
	if i%11 == 0 {
		r.NetCents = nil
	}
	return r
}

var scenarioQueries = []struct {
	source  string
	matches func(Fact) bool
}{
	{"warehouse = 'east' AND day >= 5 AND units >= 0", func(r Fact) bool { return r.Warehouse == "east" && r.Day >= 5 && r.Units != nil && *r.Units >= 0 }},
	{"category IS NULL OR net_cents >= 5000", func(r Fact) bool { return r.Category == nil || r.NetCents != nil && *r.NetCents >= 5000 }},
	{"NOT (units > 1 OR net_cents = NULL)", func(Fact) bool { return false }},
}

func stateDigest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func projectReference(r Fact) Projected {
	// The reference scans full rows and constructs a projection directly; it
	// does not use the expression AST, segment metadata or column decoders.
	v := map[Column]Value{Warehouse: text(r.Warehouse), Category: null("string"), Day: number(r.Day), Units: null("int"), NetCents: null("int")}
	if r.Category != nil {
		v[Category] = text(*r.Category)
	}
	if r.Units != nil {
		v[Units] = number(*r.Units)
	}
	if r.NetCents != nil {
		v[NetCents] = number(*r.NetCents)
	}
	return Projected{r.ID, v}
}
func referenceProjection(rows map[uint64]Fact, tenant string, predicate func(Fact) bool) []Projected {
	var result []Projected
	for _, r := range rows {
		if r.Tenant == tenant && predicate(r) {
			result = append(result, projectReference(r))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
func referenceCube(rows map[uint64]Fact, tenant string, grouping uint8) []Cell {
	// Independent direct grouping/reduction, including explicit null markers.
	m := map[CubeKey]Aggregate{}
	for _, r := range rows {
		if r.Tenant != tenant {
			continue
		}
		key := CubeKey{Tenant: tenant, Grouping: grouping}
		if grouping&1 == 0 {
			key.Warehouse = r.Warehouse
		}
		if grouping&2 == 0 && r.Category != nil {
			key.Category = *r.Category
			key.CategoryValid = true
		}
		if grouping&4 == 0 {
			key.Day = r.Day
		}
		a := m[key]
		a.Rows++
		if r.Units != nil {
			a.Units.Sum += *r.Units
			a.Units.Count++
		}
		if r.NetCents != nil {
			a.NetCents.Sum += *r.NetCents
			a.NetCents.Count++
		}
		m[key] = a
	}
	var cells []Cell
	for k, a := range m {
		cells = append(cells, Cell{k, a})
	}
	sortCells(cells)
	return cells
}
func compressionEvidence(n int) ([]Compression, error) {
	var facts []Fact
	rowBytes := 0
	for i := 0; i < n; i++ {
		r := SyntheticFact(i)
		facts = append(facts, r)
		b, _ := json.Marshal(r)
		rowBytes += len(b)
	}
	rng := rand.New(rand.NewSource(404))
	rng.Shuffle(len(facts), func(i, j int) { facts[i], facts[j] = facts[j], facts[i] })
	var result []Compression
	for _, variant := range []struct {
		name     string
		encoding StringCodec
		sorted   bool
	}{{"sorted-auto", Auto, true}, {"sorted-dictionary-rle", RLE, true}, {"sorted-dictionary-bitmap", Bitmap, true}, {"shuffled-auto", Auto, false}} {
		b, _, e := buildSegment(facts, variant.encoding, variant.sorted)
		if e != nil {
			return nil, e
		}
		if _, e = validateSegment(b); e != nil {
			return nil, e
		}
		s, e := readCatalog(b)
		if e != nil {
			return nil, e
		}
		x := Compression{Name: variant.name, Rows: n, EncodedBytes: len(b), MetadataBytes: s.payload, ColumnBytes: len(b) - s.payload, RowJSONBytes: rowBytes, Codecs: map[string]int{}}
		for _, g := range s.Groups {
			for _, c := range columns {
				kind := map[byte]string{plain64: "plain-int64", intRLE: "integer-rle", dictRLE: "dictionary-rle", dictBitmap: "dictionary-bitmap"}[g.Columns[c].Codec]
				x.Codecs[kind]++
			}
		}
		result = append(result, x)
	}
	return result, nil
}

func RunScenarioAt(ctx context.Context, parent string, rows, repetitions int) (Report, error) {
	var report Report
	if rows < 32 || rows > 4096 || repetitions < 1 || repetitions > 5 {
		return report, kv.ErrInvalid
	}
	if parent != "" {
		if e := os.MkdirAll(parent, 0700); e != nil {
			return report, e
		}
	}
	dir, e := os.MkdirTemp(parent, "atlas-columnstore-")
	if e != nil {
		return report, e
	}
	defer os.RemoveAll(dir)
	report = Report{DatasetRows: rows, Repetitions: repetitions, Environment: storage.Environment(dir)}
	report.Environment["execution"] = "scalar AST and bitmap batches in process; generated native Go predicate in a bounded subprocess; batch is not explicit SIMD"
	report.Environment["timing"] = "query wall/process CPU; ingestion and maintenance excluded; compilation separate; native query includes process/JSON overhead"
	var expressions []*Expression
	var programs []*Program
	defer func() {
		for _, p := range programs {
			p.Close()
		}
	}()
	for _, q := range scenarioQueries {
		expr, e := Parse(q.source)
		if e != nil {
			return report, e
		}
		program, e := Compile(ctx, expr, dir)
		if e != nil {
			return report, e
		}
		expressions = append(expressions, expr)
		programs = append(programs, program)
		report.Programs = append(report.Programs, ProgramEvidence{q.source, program.Source(), program.SHA256(), program.BuildNanos})
	}
	report.Compression, e = compressionEvidence(rows)
	if e != nil {
		return report, e
	}
	for rep := 0; rep < repetitions; rep++ {
		r, e := scenarioRun(ctx, filepath.Join(dir, fmt.Sprintf("facts-%d.bolt", rep)), rows, rep, expressions, programs)
		if e != nil {
			return report, e
		}
		report.Runs = append(report.Runs, r)
	}
	for i := 1; i < len(report.Runs); i++ {
		for j, m := range report.Runs[i].Measurements {
			if m.Digest != report.Runs[0].Measurements[j].Digest {
				return report, errors.New("repeated query outcomes differ")
			}
		}
	}
	report.Definitions = []string{
		"synthetic warehouse facts; mandatory tenant equality; per-ID strictly increasing versions; heads/tombstones resolve visibility before filtering",
		"immutable 64-row groups, up to 4096 rows/segment, sorted by tenant/warehouse/day/category/ID; dictionary/RLE/bitmap strings and plain/RLE int64 columns with validity bitmaps",
		"bbolt publishes delta, heads and all eight tenant-preserving cube groupings in one synchronous transaction; flush/compaction publish segments atomically",
		"column_bytes counts encoded column frames actually decoded; metadata and full delta JSON bytes are separate; bbolt mmap/filesystem/device bytes are not measured",
		"row reference operates on already materialized full rows; its logical JSON scan bytes are a representation baseline, not device I/O or a matched row-decoding benchmark",
		"query process CPU is Linux process user+system time delta and includes runtime work; compiled child CPU is reported separately; other platforms omit unavailable CPU counters",
		"native compiled predicates come only from the bounded typed grammar; Go source and its hash are retained; compilation is separate, while subprocess startup and JSON protocol are included in compiled query time",
		"batch evaluates column vectors and uint64 selection/unknown masks using Go loops and bitwise operations; no explicit SIMD instructions or in-process JIT are claimed",
		"raw repetitions describe warm process/OS cache; no wall-clock speed ranking, cold-device claim or cloud warehouse implementation is inferred",
	}
	report.Passed = true
	return report, nil
}
func scenarioRun(ctx context.Context, path string, n, rep int, expressions []*Expression, programs []*Program) (run ScenarioRun, err error) {
	run = ScenarioRun{Repetition: rep, Phases: map[string]Stats{}}
	d, e := Open(path, Options{})
	if e != nil {
		return run, e
	}
	defer func() {
		if d != nil {
			err = errors.Join(err, d.Close())
		}
	}()
	oracle := map[uint64]Fact{}
	versions := map[uint64]uint64{}
	ids := rand.New(rand.NewSource(int64(404 + rep))).Perm(n)
	for _, i := range ids {
		r := SyntheticFact(i)
		if e = d.Put(r); e != nil {
			return run, e
		}
		oracle[r.ID] = r
		versions[r.ID] = 1
	}
	if e = d.Flush(); e != nil {
		return run, e
	}
	measure := func(phase string) error {
		if e := d.Check(); e != nil {
			return e
		}
		s, e := d.Stats()
		if e != nil {
			return e
		}
		run.Phases[phase] = s
		for grouping := uint8(0); grouping < 8; grouping++ {
			for _, tenant := range []string{"acme", "globex"} {
				got, e := d.Materialized(tenant, grouping)
				if e != nil {
					return e
				}
				if got.StateVersion != s.StateVersion || !reflect.DeepEqual(got.Cells, referenceCube(oracle, tenant, grouping)) {
					return errors.New("incremental cube differs from full row reduction")
				}
			}
		}
		rowBytes := 0
		for _, r := range oracle {
			b, _ := json.Marshal(r)
			rowBytes += len(b)
		}
		for qi, q := range scenarioQueries {
			start := time.Now()
			cpu := processCPU()
			want := referenceProjection(oracle, "acme", q.matches)
			elapsed := time.Since(start).Nanoseconds()
			var referenceCPU *int64
			if end := processCPU(); end != nil && cpu != nil {
				n := *end - *cpu
				referenceCPU = &n
			}
			// Warm once per mode; compiled queries still start a new native process.
			for _, mode := range []Mode{Scalar, Batch, Compiled} {
				query := Query{Tenant: "acme", Filter: expressions[qi], Select: []Column{Warehouse, Category, Day, Units, NetCents}, Mode: mode, Program: programs[qi]}
				if _, e = d.Query(ctx, query); e != nil {
					return e
				}
				got, e := d.Query(ctx, query)
				if e != nil {
					return e
				}
				if !reflect.DeepEqual(got.Rows, want) {
					return errors.New("execution mode differs from independent row reference")
				}
				cells, e := AggregateResult(got, AllDays)
				if e != nil {
					return e
				}
				filtered := map[uint64]Fact{}
				for id, r := range oracle {
					if r.Tenant == "acme" && q.matches(r) {
						filtered[id] = r
					}
				}
				if !reflect.DeepEqual(cells, referenceCube(filtered, "acme", AllDays)) {
					return errors.New("filtered aggregate differs")
				}
				run.Measurements = append(run.Measurements, Measurement{phase, q.source, mode, len(got.Rows), stateDigest(got.Rows), stateDigest(cells), got.StateVersion, got.Stats, rowBytes, elapsed, referenceCPU})
			}
		}
		return nil
	}
	if e = measure("sealed"); e != nil {
		return run, e
	}
	for i := 0; i < n; i += 9 {
		r := SyntheticFact(i)
		versions[r.ID]++
		r.Version = versions[r.ID]
		r.Warehouse = "north"
		r.Day += 10
		r.Units = nil
		if e = d.Put(r); e != nil {
			return run, e
		}
		oracle[r.ID] = r
	}
	for i := 0; i < n; i += 13 {
		r := SyntheticFact(i)
		versions[r.ID]++
		if e = d.Delete(r.Tenant, r.ID, versions[r.ID]); e != nil {
			return run, e
		}
		delete(oracle, r.ID)
	}
	for i := n; i < n+n/16; i++ {
		r := SyntheticFact(i)
		if e = d.Put(r); e != nil {
			return run, e
		}
		oracle[r.ID] = r
	}
	if e = measure("delta"); e != nil {
		return run, e
	}
	if e = d.Flush(); e != nil {
		return run, e
	}
	if e = d.Compact(); e != nil {
		return run, e
	}
	before, e := d.Materialized("acme", AllWarehouses|AllCategories|AllDays)
	if e != nil {
		return run, e
	}
	if e = d.RebuildCube(); e != nil {
		return run, e
	}
	after, e := d.Materialized("acme", 7)
	if e != nil || !reflect.DeepEqual(before, after) {
		return run, errors.New("cube rebuild changed logical aggregate")
	}
	if e = measure("compacted"); e != nil {
		return run, e
	}
	if e = d.Close(); e != nil {
		return run, e
	}
	d = nil
	d, e = Open(path, Options{})
	if e != nil {
		return run, e
	}
	if e = measure("reopened"); e != nil {
		return run, e
	}
	run.ReopenVerified = true
	run.CubeVerified = true
	return run, nil
}
