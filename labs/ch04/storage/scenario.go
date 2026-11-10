// Package storage runs synthetic warehouse snapshot workloads over independent
// embedded stores. It never replaces or imports an authoritative app database.
package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/btree"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/embedded"
	storekv "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/lsm"
)

type kvStore = storekv.Store
type Run struct {
	WriteNanos           int64              `json:"write_nanos"`
	ReadNanos            int64              `json:"read_nanos"`
	WriteP50Nanos        int64              `json:"write_p50_nanos"`
	WriteP95Nanos        int64              `json:"write_p95_nanos"`
	DiskBytes            int64              `json:"disk_bytes"`
	LiveBytes            int64              `json:"live_key_value_bytes"`
	Digest               string             `json:"state_sha256"`
	LSM                  *lsm.Stats         `json:"lsm,omitempty"`
	BTree                *btree.Stats       `json:"btree,omitempty"`
	BTreeCheck           *btree.CheckReport `json:"btree_check,omitempty"`
	AfterChurn           *btree.Stats       `json:"after_delete_reopen_reinsert,omitempty"`
	FreeReuseVerified    bool               `json:"free_page_reuse_verified,omitempty"`
	AfterCompactionBytes int64              `json:"after_full_compaction_bytes,omitempty"`
	ReopenVerified       bool               `json:"reopen_verified"`
}
type Result struct {
	Engine        string         `json:"engine"`
	Durability    string         `json:"durability"`
	Configuration map[string]any `json:"configuration"`
	Runs          []Run          `json:"runs"`
}
type Report struct {
	Passed      bool           `json:"passed"`
	DatasetRows int            `json:"dataset_rows"`
	Repetitions int            `json:"repetitions"`
	Environment map[string]any `json:"environment"`
	Results     []Result       `json:"results"`
	Assertions  []string       `json:"assertions"`
}

func key(i int, missing bool) []byte {
	tenant := "acme"
	if i%2 != 0 {
		tenant = "globex"
	}
	warehouse := "east"
	if i%3 == 0 {
		warehouse = "west"
	}
	suffix := 2 * i
	if missing {
		suffix++
	}
	return []byte(fmt.Sprintf("tenant/%s/warehouse/%s/sku/%06d", tenant, warehouse, suffix))
}
func value(i, revision int) []byte {
	b, _ := json.Marshal(struct{ OnHand, Reserved, Allocated, Version int }{100 + i, 2, 1, revision})
	return b
}

func RunScenario(rows, repetitions int) (Report, error) {
	return RunScenarioAt("", rows, repetitions)
}

// RunScenarioAt creates only owned temporary children of parent. This lets the
// caller choose a filesystem without ever deleting an existing database.
func RunScenarioAt(parent string, rows, repetitions int) (Report, error) {
	return runEngines(parent, rows, repetitions, []string{"append-log", "lsm-tiered", "lsm-leveled", "btree", "bbolt"})
}
func RunBTreeScenarioAt(parent string, rows, repetitions int) (Report, error) {
	return runEngines(parent, rows, repetitions, []string{"btree", "bbolt"})
}
func runEngines(parent string, rows, repetitions int, engines []string) (Report, error) {
	var report Report
	if rows < 32 || rows > 4096 || repetitions < 1 || repetitions > 5 {
		return report, lsm.ErrInvalid
	}
	if parent != "" {
		if e := os.MkdirAll(parent, 0700); e != nil {
			return report, e
		}
	}
	dir, e := os.MkdirTemp(parent, "atlas-storage-")
	if e != nil {
		return report, e
	}
	defer os.RemoveAll(dir)
	report = Report{DatasetRows: rows, Repetitions: repetitions, Environment: Environment(dir)}
	for _, engine := range engines {
		result := Result{Engine: engine, Durability: "sync every mutation", Configuration: configuration(engine)}
		for run := 0; run < repetitions; run++ {
			path := filepath.Join(dir, fmt.Sprintf("%s-%d", engine, run))
			if e = os.MkdirAll(path, 0700); e != nil {
				return report, e
			}
			measurement, e := runStore(path, engine, rows)
			if e != nil {
				return report, fmt.Errorf("%s run %d: %w", engine, run, e)
			}
			result.Runs = append(result.Runs, measurement)
		}
		report.Results = append(report.Results, result)
	}
	// All engines and all repeated runs must represent exactly the same state.
	digest := report.Results[0].Runs[0].Digest
	for _, result := range report.Results {
		for _, r := range result.Runs {
			if r.Digest != digest {
				return report, errors.New("store outcomes disagree")
			}
		}
	}
	report.Passed = true
	report.Assertions = []string{"warehouse snapshot updates and deletes match the oracle", "tenant-prefix scans match the oracle", "missing keys and empty values remain distinct", "reopening retains the same logical state", "selected engines have identical state digests"}
	for _, engine := range engines {
		if engine == "btree" {
			report.Assertions = append(report.Assertions, "B+ tree occupancy, separators, links and allocation are valid", "free pages survive reopen and are reused after deleting and reinserting the live set")
		}
		if engine == "lsm-leveled" {
			report.Assertions = append(report.Assertions, "full LSM compaction preserves state and removes deleted versions")
		}
	}
	return report, nil
}

// Environment describes the actual report directory's filesystem, without
// treating a container filesystem as an identified physical storage device.
func Environment(dir string) map[string]any {
	return map[string]any{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "filesystem": filesystemInfo(dir), "cache": "fresh database; warm process and OS cache; no cache eviction", "timing": "wall clock; includes sync and compaction; no device-level claims", "bbolt_version": moduleVersion("go.etcd.io/bbolt")}
}
func moduleVersion(path string) string {
	if b, ok := debug.ReadBuildInfo(); ok {
		for _, d := range b.Deps {
			if d.Path == path {
				return d.Version
			}
		}
	}
	return "unknown"
}
func openStore(dir, engine string) (kvStore, error) {
	if engine == "btree" {
		return btree.Open(dir, btree.Options{})
	}
	if engine == "bbolt" {
		return embedded.OpenBolt(filepath.Join(dir, "data.bolt"))
	}
	return lsm.Open(dir, lsmOptions(engine))
}
func lsmOptions(engine string) lsm.Options {
	o := lsm.Options{MemtableBytes: 8192, BlockBytes: 4096, TargetTableBytes: 16384, Fanout: 4, Style: lsm.Leveled, Durability: lsm.SyncWrites}
	if engine == "append-log" {
		o.MemtableBytes = 16 << 20
		o.DisableAutoCompaction = true
	}
	if engine == "lsm-tiered" {
		o.Style = lsm.Tiered
	}
	return o
}
func configuration(engine string) map[string]any {
	if engine == "btree" {
		return map[string]any{"layout": "in-place B+ tree", "page_bytes": btree.PageSize, "max_keys": 8, "max_key_bytes": btree.MaxKeyBytes, "max_value_bytes": btree.MaxValueBytes, "transaction": "full-page redo with synchronous checkpoint"}
	}
	if engine == "bbolt" {
		return map[string]any{"layout": "copy-on-write B+ tree", "transaction": "one per mutation", "no_sync": false}
	}
	o := lsmOptions(engine)
	return map[string]any{"memtable_bytes": o.MemtableBytes, "block_bytes": o.BlockBytes, "target_table_bytes": o.TargetTableBytes, "fanout": o.Fanout, "style": o.Style, "auto_compaction": !o.DisableAutoCompaction, "max_key_bytes": lsm.MaxKeyBytes, "max_value_bytes": lsm.MaxValueBytes}
}
func runStore(dir, engine string, n int) (result Run, err error) {
	db, e := openStore(dir, engine)
	if e != nil {
		return result, e
	}
	defer func() {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	oracle := map[string][]byte{}
	var latencies []int64
	measure := func(f func() error) error {
		start := time.Now()
		e := f()
		latencies = append(latencies, time.Since(start).Nanoseconds())
		return e
	}
	start := time.Now()
	for i := 0; i < n; i++ {
		k, v := key(i, false), value(i, 1)
		if e = measure(func() error { return db.Put(k, v) }); e != nil {
			return result, e
		}
		oracle[string(k)] = v
	}
	for i := 0; i < n; i += 3 {
		k, v := key(i, false), value(i, 2)
		if e = measure(func() error { return db.Put(k, v) }); e != nil {
			return result, e
		}
		oracle[string(k)] = v
	}
	for i := 0; i < n; i += 7 {
		k := key(i, false)
		if e = measure(func() error { return db.Delete(k) }); e != nil {
			return result, e
		}
		delete(oracle, string(k))
	}
	if engine != "append-log" {
		if l, ok := db.(*lsm.DB); ok {
			if e = l.Flush(); e != nil {
				return result, e
			}
		}
	}
	result.WriteNanos = time.Since(start).Nanoseconds()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	result.WriteP50Nanos = latencies[len(latencies)/2]
	result.WriteP95Nanos = latencies[(len(latencies)-1)*95/100]
	start = time.Now()
	if e = verify(db, oracle, n); e != nil {
		return result, e
	}
	result.ReadNanos = time.Since(start).Nanoseconds()
	for k, v := range oracle {
		result.LiveBytes += int64(len(k) + len(v))
	}
	result.Digest = digest(oracle)
	if l, ok := db.(*lsm.DB); ok {
		s, e := l.Stats()
		if e != nil {
			return result, e
		}
		result.LSM = &s
		result.DiskBytes = s.DiskBytes
	} else if tree, ok := db.(*btree.DB); ok {
		check, e := tree.Check()
		if e != nil {
			return result, e
		}
		result.BTreeCheck = &check
		s, e := tree.Stats()
		if e != nil {
			return result, e
		}
		result.BTree = &s
		result.DiskBytes = s.DiskBytes
	} else {
		result.DiskBytes, e = db.(*embedded.Bolt).DiskBytes()
		if e != nil {
			return result, e
		}
	}
	if e = db.Close(); e != nil {
		return result, e
	}
	db = nil
	db, e = openStore(dir, engine)
	if e != nil {
		return result, e
	}
	if e = verify(db, oracle, n); e != nil {
		return result, e
	}
	result.ReopenVerified = true
	if e = emptyValueProbe(db); e != nil {
		return result, e
	}
	if tree, ok := db.(*btree.DB); ok {
		rows := sortedOracle(oracle, nil)
		for _, p := range rows {
			if e = tree.Delete(p.Key); e != nil {
				return result, e
			}
		}
		check, e := tree.Check()
		if e != nil {
			return result, e
		}
		if check.Keys != 0 || check.Height != 1 || check.FreePages == 0 {
			return result, errors.New("tree did not collapse into reusable free pages")
		}
		if e = db.Close(); e != nil {
			return result, e
		}
		db = nil
		db, e = openStore(dir, engine)
		if e != nil {
			return result, e
		}
		tree = db.(*btree.DB)
		check, e = tree.Check()
		if e != nil {
			return result, e
		}
		if check.Keys != 0 || check.FreePages == 0 {
			return result, errors.New("free list did not survive reopening")
		}
		for _, p := range rows {
			if e = tree.Put(p.Key, p.Value); e != nil {
				return result, e
			}
		}
		if e = verify(tree, oracle, n); e != nil {
			return result, e
		}
		if _, e = tree.Check(); e != nil {
			return result, e
		}
		after, e := tree.Stats()
		if e != nil {
			return result, e
		}
		if after.Counters.FreeReuses == 0 {
			return result, errors.New("free pages were not reused")
		}
		result.FreeReuseVerified = true
		result.AfterChurn = &after
	}
	if l, ok := db.(*lsm.DB); ok && engine != "append-log" {
		if e = l.Compact(); e != nil {
			return result, e
		}
		if e = verify(l, oracle, n); e != nil {
			return result, e
		}
		s, e := l.Stats()
		if e != nil {
			return result, e
		}
		result.AfterCompactionBytes = s.DiskBytes
	}
	return result, nil
}
func verify(db kvStore, oracle map[string][]byte, n int) error {
	for i := 0; i < n; i++ {
		k := key(i, false)
		want, ok := oracle[string(k)]
		got, e := db.Get(k)
		if ok && (e != nil || !bytes.Equal(got, want)) || !ok && !errors.Is(e, lsm.ErrNotFound) {
			return errors.New("point read differs from oracle")
		}
		if _, e = db.Get(key(i, true)); !errors.Is(e, lsm.ErrNotFound) {
			return errors.New("absent key became visible")
		}
	}
	for _, prefix := range [][]byte{nil, []byte("tenant/acme/"), []byte("tenant/globex/")} {
		got, e := db.Scan(prefix, lsm.PrefixEnd(prefix), 65536)
		if e != nil {
			return e
		}
		want := sortedOracle(oracle, prefix)
		if len(got) != len(want) {
			return errors.New("range count differs from oracle")
		}
		for i := range got {
			if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
				return errors.New("range ordering or values differ")
			}
		}
	}
	return nil
}

// Probe after measurements so read timing and write counters stay comparable.
func emptyValueProbe(db kvStore) error {
	empty := []byte("empty-value-probe")
	if e := db.Put(empty, nil); e != nil {
		return e
	}
	if v, e := db.Get(empty); e != nil || len(v) != 0 {
		return errors.New("empty value became absent")
	}
	return db.Delete(empty)
}
func sortedOracle(m map[string][]byte, prefix []byte) []lsm.Pair {
	var keys []string
	for k := range m {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var r []lsm.Pair
	for _, k := range keys {
		r = append(r, lsm.Pair{Key: []byte(k), Value: m[k]})
	}
	return r
}
func digest(m map[string][]byte) string {
	h := sha256.New()
	for _, p := range sortedOracle(m, nil) {
		var lengths [8]byte
		binary.LittleEndian.PutUint32(lengths[:4], uint32(len(p.Key)))
		binary.LittleEndian.PutUint32(lengths[4:], uint32(len(p.Value)))
		h.Write(lengths[:])
		h.Write(p.Key)
		h.Write(p.Value)
	}
	return hex.EncodeToString(h.Sum(nil))
}
