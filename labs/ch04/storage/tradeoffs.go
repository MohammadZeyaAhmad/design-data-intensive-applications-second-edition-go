package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/btree"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/embedded"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/lsm"
)

// EngineIO reports bytes requested by the custom engines, not block-device I/O.
// Metadata syscalls, filesystem journals, and OS/device cache effects are absent.
type EngineIO struct {
	WriteBytes    uint64 `json:"write_bytes_requested"`
	ReadBytes     uint64 `json:"read_bytes_requested"`
	VerifiedBytes uint64 `json:"open_table_verification_bytes"`
	Syncs         uint64 `json:"sync_calls"`
}
type ReadMeasurement struct {
	Kind          string    `json:"kind"`
	Queries       int       `json:"queries"`
	Returned      int       `json:"returned_rows"`
	UsefulBytes   uint64    `json:"returned_key_value_bytes"`
	Nanos         int64     `json:"nanos"`
	IO            *EngineIO `json:"engine_io,omitempty"`
	BytesPerQuery *float64  `json:"requested_bytes_per_query,omitempty"`
	Amplification *float64  `json:"requested_bytes_per_returned_byte,omitempty"`
}
type TradeoffRun struct {
	Engine             string            `json:"engine"`
	WriteOrder         string            `json:"write_order"`
	Repetition         int               `json:"repetition"`
	ScheduleSeed       int64             `json:"schedule_seed"`
	WriteNanos         int64             `json:"write_nanos_including_flush"`
	P50Nanos           int64             `json:"mutation_p50_nanos"`
	P95Nanos           int64             `json:"mutation_p95_nanos"`
	Mutations          int               `json:"mutations"`
	LogicalBytes       uint64            `json:"mutated_key_value_bytes"`
	LiveBytes          int64             `json:"live_key_value_bytes"`
	DiskBytes          int64             `json:"disk_bytes"`
	WriteIO            *EngineIO         `json:"write_phase_io,omitempty"`
	WriteAmplification *float64          `json:"requested_write_amplification,omitempty"`
	SpaceAmplification float64           `json:"file_space_amplification"`
	Warm               []ReadMeasurement `json:"warm_reads"`
	ReopenNanos        int64             `json:"reopen_nanos_including_validation"`
	ReopenIO           *EngineIO         `json:"reopen_io,omitempty"`
	Reopened           []ReadMeasurement `json:"reopened_handle_reads"`
	CompactionNanos    int64             `json:"full_compaction_nanos,omitempty"`
	CompactionIO       *EngineIO         `json:"full_compaction_io,omitempty"`
	CompactedDiskBytes int64             `json:"compacted_disk_bytes,omitempty"`
	Digest             string            `json:"state_sha256"`
	Configuration      map[string]any    `json:"configuration"`
}
type Distribution struct {
	Count  int     `json:"count"`
	Min    int64   `json:"min"`
	Max    int64   `json:"max"`
	Median int64   `json:"median"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"population_stddev"`
	CV     float64 `json:"coefficient_of_variation"`
}
type TradeoffSummary struct {
	Engine        string       `json:"engine"`
	WriteOrder    string       `json:"write_order"`
	WriteNanos    Distribution `json:"write_nanos"`
	WarmReadNanos Distribution `json:"warm_read_nanos"`
}
type TradeoffReport struct {
	Passed      bool              `json:"passed"`
	DatasetRows int               `json:"dataset_rows"`
	Repetitions int               `json:"repetitions"`
	Environment map[string]any    `json:"environment"`
	CacheModes  map[string]string `json:"cache_modes"`
	Results     []TradeoffRun     `json:"results"`
	Summary     []TradeoffSummary `json:"summary"`
	Definitions []string          `json:"definitions"`
}

func distribution(values []int64) Distribution {
	d := Distribution{Count: len(values)}
	if len(values) == 0 {
		return d
	}
	v := append([]int64(nil), values...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	d.Min = v[0]
	d.Max = v[len(v)-1]
	d.Median = v[len(v)/2]
	for _, x := range v {
		d.Mean += float64(x) / float64(len(v))
	}
	for _, x := range v {
		delta := float64(x) - d.Mean
		d.StdDev += delta * delta / float64(len(v))
	}
	d.StdDev = math.Sqrt(d.StdDev)
	if d.Mean != 0 {
		d.CV = d.StdDev / d.Mean
	}
	return d
}

func observe(db kv.Store) (*EngineIO, int64, error) {
	switch d := db.(type) {
	case *lsm.DB:
		s, e := d.Stats()
		c := s.Counters
		return &EngineIO{WriteBytes: c.WALBytes + c.TableBytes + c.ManifestBytes, ReadBytes: c.ReadBytes, VerifiedBytes: c.VerifiedBytes, Syncs: c.Syncs}, s.DiskBytes, e
	case *btree.DB:
		s, e := d.Stats()
		c := s.Counters
		return &EngineIO{WriteBytes: c.WALBytes + c.DataBytes, ReadBytes: c.ReadBytes, Syncs: c.Syncs}, s.DiskBytes, e
	case *embedded.Bolt:
		n, e := d.DiskBytes()
		return nil, n, e
	default:
		return nil, 0, kv.ErrInvalid
	}
}
func deltaIO(a, b *EngineIO) *EngineIO {
	if a == nil || b == nil {
		return nil
	}
	return &EngineIO{WriteBytes: b.WriteBytes - a.WriteBytes, ReadBytes: b.ReadBytes - a.ReadBytes, VerifiedBytes: b.VerifiedBytes - a.VerifiedBytes, Syncs: b.Syncs - a.Syncs}
}
func ratio(a, b uint64) *float64 {
	if b == 0 {
		return nil
	}
	f := float64(a) / float64(b)
	return &f
}

// RunTradeoffsAt rotates engine execution order and uses owned fresh databases.
// Reopened handles retain the OS cache: both engines validate data at Open.
func RunTradeoffsAt(parent string, rows, repetitions int) (TradeoffReport, error) {
	var report TradeoffReport
	if rows < 32 || rows > 4096 || repetitions < 1 || repetitions > 5 {
		return report, kv.ErrInvalid
	}
	if parent != "" {
		if e := os.MkdirAll(parent, 0700); e != nil {
			return report, e
		}
	}
	dir, e := os.MkdirTemp(parent, "atlas-tradeoffs-")
	if e != nil {
		return report, e
	}
	defer os.RemoveAll(dir)
	report = TradeoffReport{DatasetRows: rows, Repetitions: repetitions, Environment: Environment(dir), CacheModes: map[string]string{
		"warm":            "full oracle verification precedes the measured queries; process and OS cache retained",
		"reopened_handle": "close/open, then measure; OS cache retained; Open validates pages/tables and warms data",
		"cold_os_cache":   "not measured; no global cache eviction or claim that reopen makes data cold",
	}}
	engines := []string{"lsm-tiered", "lsm-leveled", "btree", "bbolt"}
	var expected string
	for rep := 0; rep < repetitions; rep++ {
		for _, order := range []string{"sequential", "random"} {
			for offset := 0; offset < len(engines); offset++ {
				engine := engines[(rep+offset)%len(engines)]
				path := filepath.Join(dir, fmt.Sprintf("%s-%s-%d", engine, order, rep))
				if e = os.Mkdir(path, 0700); e != nil {
					return report, e
				}
				r, e := tradeoffRun(path, engine, order, rows, rep)
				if e != nil {
					return report, fmt.Errorf("%s/%s/%d: %w", engine, order, rep, e)
				}
				if expected == "" {
					expected = r.Digest
				}
				if r.Digest != expected {
					return report, errors.New("write schedules or engines disagree")
				}
				report.Results = append(report.Results, r)
			}
		}
	}
	for _, engine := range engines {
		for _, order := range []string{"sequential", "random"} {
			var writes, reads []int64
			for _, r := range report.Results {
				if r.Engine == engine && r.WriteOrder == order {
					writes = append(writes, r.WriteNanos)
					var total int64
					for _, p := range r.Warm {
						total += p.Nanos
					}
					reads = append(reads, total)
				}
			}
			report.Summary = append(report.Summary, TradeoffSummary{engine, order, distribution(writes), distribution(reads)})
		}
	}
	report.Definitions = []string{
		"same logical insert/update/delete set for every engine, order and repetition; mutation order is sorted byte-key order or a recorded deterministic shuffle",
		"sync every accepted mutation; one bbolt transaction per mutation; inline automatic LSM compaction and final memtable flush included in write time",
		"write amplification = WAL + table/manifest or DATA bytes requested after initialization, divided by submitted key/value bytes (delete contributes key bytes)",
		"read amplification = custom-engine bytes requested during only that query phase / returned key+value bytes; misses instead report bytes/query",
		"space amplification = current logical file lengths / live key+value bytes; includes metadata, free pages and retained versions, excludes filesystem allocation/journals",
		"bbolt byte-request instrumentation unavailable; omitted ratios are not zero; all stores report time and file lengths",
		"reopen validation is timed separately; full LSM compaction is an additional measured phase, not silently excluded maintenance",
		"raw measurements, population variance and coefficient of variation describe this workload only; small B+ fanout and container filesystem preclude device rankings",
	}
	report.Passed = true
	return report, nil
}

func tradeoffRun(dir, engine, writeOrder string, n, rep int) (r TradeoffRun, err error) {
	r = TradeoffRun{Engine: engine, WriteOrder: writeOrder, Repetition: rep, ScheduleSeed: int64(410300 + rep), Configuration: configuration(engine)}
	db, e := openStore(dir, engine)
	if e != nil {
		return r, e
	}
	defer func() {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	before, _, e := observe(db)
	if e != nil {
		return r, e
	}
	oracle := map[string][]byte{}
	var latencies []int64
	rng := rand.New(rand.NewSource(r.ScheduleSeed))
	schedule := func(step int) []int {
		var ids []int
		for i := 0; i < n; i += step {
			ids = append(ids, i)
		}
		sort.Slice(ids, func(i, j int) bool { return bytes.Compare(key(ids[i], false), key(ids[j], false)) < 0 })
		if writeOrder == "random" {
			rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		}
		return ids
	}
	type mutation struct {
		key, value []byte
		deleted    bool
	}
	var mutations []mutation
	for phase, step := range []int{1, 4, 7} {
		for _, i := range schedule(step) {
			k := key(i, false)
			v := value(i, phase+1)
			if phase == 2 {
				delete(oracle, string(k))
				v = nil
			} else {
				oracle[string(k)] = v
			}
			mutations = append(mutations, mutation{k, v, phase == 2})
			r.LogicalBytes += uint64(len(k) + len(v))
		}
	}
	start := time.Now()
	for _, m := range mutations {
		t := time.Now()
		if m.deleted {
			e = db.Delete(m.key)
		} else {
			e = db.Put(m.key, m.value)
		}
		elapsed := time.Since(t).Nanoseconds()
		if e != nil {
			return r, e
		}
		latencies = append(latencies, elapsed)
		r.Mutations++
	}
	if l, ok := db.(*lsm.DB); ok {
		if e = l.Flush(); e != nil {
			return r, e
		}
	}
	r.WriteNanos = time.Since(start).Nanoseconds()
	after, disk, e := observe(db)
	if e != nil {
		return r, e
	}
	r.WriteIO = deltaIO(before, after)
	r.DiskBytes = disk
	if r.WriteIO != nil {
		r.WriteAmplification = ratio(r.WriteIO.WriteBytes, r.LogicalBytes)
	}
	for k, v := range oracle {
		r.LiveBytes += int64(len(k) + len(v))
	}
	r.SpaceAmplification = float64(disk) / float64(r.LiveBytes)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	r.P50Nanos = latencies[len(latencies)/2]
	r.P95Nanos = latencies[(len(latencies)-1)*95/100]
	if e = verify(db, oracle, n); e != nil {
		return r, e
	}
	r.Digest = digest(oracle)
	r.Warm, e = measureReads(db, oracle, n)
	if e != nil {
		return r, e
	}
	if e = db.Close(); e != nil {
		return r, e
	}
	db = nil
	start = time.Now()
	db, e = openStore(dir, engine)
	r.ReopenNanos = time.Since(start).Nanoseconds()
	if e != nil {
		return r, e
	}
	r.ReopenIO, _, e = observe(db)
	if e != nil {
		return r, e
	}
	r.Reopened, e = measureReads(db, oracle, n)
	if e != nil {
		return r, e
	}
	if e = verify(db, oracle, n); e != nil {
		return r, e
	}
	if l, ok := db.(*lsm.DB); ok {
		before, _, e = observe(db)
		if e != nil {
			return r, e
		}
		start = time.Now()
		if e = l.Compact(); e != nil {
			return r, e
		}
		r.CompactionNanos = time.Since(start).Nanoseconds()
		after, r.CompactedDiskBytes, e = observe(db)
		if e != nil {
			return r, e
		}
		r.CompactionIO = deltaIO(before, after)
		if e = verify(db, oracle, n); e != nil {
			return r, e
		}
	}
	return r, nil
}

func measureReads(db kv.Store, oracle map[string][]byte, n int) ([]ReadMeasurement, error) {
	var results []ReadMeasurement
	for _, kind := range []string{"point-hit", "point-miss", "bounded-range"} {
		before, _, e := observe(db)
		if e != nil {
			return nil, e
		}
		r := ReadMeasurement{Kind: kind}
		switch kind {
		case "point-hit":
			ids := rand.New(rand.NewSource(4103)).Perm(n)
			for _, i := range ids {
				k := key(i, false)
				want, ok := oracle[string(k)]
				if !ok {
					continue
				}
				start := time.Now()
				got, e := db.Get(k)
				r.Nanos += time.Since(start).Nanoseconds()
				if e != nil || !bytes.Equal(got, want) {
					return nil, errors.New("measured hit differs from oracle")
				}
				r.Queries++
				r.Returned++
				r.UsefulBytes += uint64(len(k) + len(got))
			}
		case "point-miss":
			for i := 0; i < n; i++ {
				k := key(i, true)
				start := time.Now()
				_, e = db.Get(k)
				r.Nanos += time.Since(start).Nanoseconds()
				if !errors.Is(e, kv.ErrNotFound) {
					return nil, errors.New("measured miss differs from oracle")
				}
				r.Queries++
			}
		case "bounded-range":
			all := sortedOracle(oracle, nil)
			for i := 0; i < len(all); i += 17 {
				end := min(i+16, len(all))
				var upper []byte
				if end < len(all) {
					upper = all[end].Key
				}
				start := time.Now()
				got, e := db.Scan(all[i].Key, upper, 16)
				r.Nanos += time.Since(start).Nanoseconds()
				if e != nil {
					return nil, e
				}
				if len(got) != end-i {
					return nil, errors.New("measured range count differs")
				}
				for j, p := range got {
					if !bytes.Equal(p.Key, all[i+j].Key) || !bytes.Equal(p.Value, all[i+j].Value) {
						return nil, errors.New("measured range differs from oracle")
					}
					r.Returned++
					r.UsefulBytes += uint64(len(p.Key) + len(p.Value))
				}
				r.Queries++
			}
		}
		after, _, e := observe(db)
		if e != nil {
			return nil, e
		}
		r.IO = deltaIO(before, after)
		if r.IO != nil {
			r.BytesPerQuery = ratio(r.IO.ReadBytes, uint64(r.Queries))
			r.Amplification = ratio(r.IO.ReadBytes, r.UsefulBytes)
		}
		results = append(results, r)
	}
	return results, nil
}
