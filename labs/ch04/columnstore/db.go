package columnstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	bolt "go.etcd.io/bbolt"
)

var metaBucket = []byte("metadata")
var deltaBucket = []byte("delta")
var headBucket = []byte("heads")
var segmentBucket = []byte("segments")
var cubeBucket = []byte("cube")

type Options struct {
	// Encoding is persisted. Empty adopts an existing store, or defaults to auto.
	Encoding StringCodec
	// Failpoint runs at ingest/flush/compact/cube.ready before commit, and at
	// *.committed afterward. Do not re-enter DB. Postcommit errors poison it.
	Failpoint func(string) error
}
type metadata struct {
	Format                                     int
	Encoding                                   StringCodec
	State, CubeState, Publication, NextSegment uint64
	Heads, Live, Delta                         int
	Segments                                   []uint64
}
type head struct {
	Tenant        string
	Version       uint64
	Deleted       bool
	Segment       uint64
	Group, Offset int
}
type delta struct {
	ID, Version uint64
	Deleted     bool
	Row         *Fact
}
type DB struct {
	mu      sync.RWMutex
	disk    *bolt.DB
	path    string
	options Options
	closed  bool
	bad     error
}
type Stats struct {
	StateVersion uint64 `json:"state_version"`
	Publication  uint64 `json:"publication"`
	Heads        int    `json:"version_heads_including_tombstones"`
	Live         int    `json:"live_facts"`
	Delta        int    `json:"delta_records"`
	Segments     int    `json:"segments"`
	EncodedBytes int    `json:"segment_bytes"`
	DeltaBytes   int    `json:"delta_json_bytes"`
	CubeCells    int    `json:"cube_cells"`
	FileBytes    int64  `json:"file_bytes"`
}

func idBytes(id uint64) []byte { return binary.BigEndian.AppendUint64(nil, id) }
func decodeMeta(tx *bolt.Tx) (metadata, error) {
	var m metadata
	b := tx.Bucket(metaBucket)
	if b == nil || json.Unmarshal(b.Get([]byte("state")), &m) != nil {
		return m, kv.ErrCorrupt
	}
	if m.Format != 1 || m.Encoding != Auto && m.Encoding != RLE && m.Encoding != Bitmap || m.NextSegment == 0 || m.Heads < 0 || m.Heads > MaxIDs || m.Live < 0 || m.Live > m.Heads || m.Delta < 0 || m.Delta > MaxDelta || len(m.Segments) > MaxSegments || m.CubeState != m.State || m.Publication < m.State || uint64(m.Heads) > m.State {
		return m, kv.ErrCorrupt
	}
	previous := uint64(0)
	for _, id := range m.Segments {
		if id <= previous || id >= m.NextSegment {
			return m, kv.ErrCorrupt
		}
		previous = id
	}
	return m, nil
}
func saveMeta(tx *bolt.Tx, m metadata) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	return tx.Bucket(metaBucket).Put([]byte("state"), b)
}
func saveHead(tx *bolt.Tx, id uint64, h head) error {
	b, _ := json.Marshal(h)
	return tx.Bucket(headBucket).Put(idBytes(id), b)
}
func readHead(tx *bolt.Tx, id uint64) (head, bool, error) {
	var h head
	b := tx.Bucket(headBucket).Get(idBytes(id))
	if b == nil {
		return h, false, nil
	}
	if json.Unmarshal(b, &h) != nil || !validName(h.Tenant) || h.Version == 0 || h.Version > 1e12 || h.Group < 0 || h.Group >= SegmentRows/GroupRows || h.Offset < 0 || h.Offset >= GroupRows || h.Deleted && h.Segment != 0 || h.Segment == 0 && (h.Group != 0 || h.Offset != 0) {
		return h, true, kv.ErrCorrupt
	}
	return h, true, nil
}
func readDelta(b []byte) (delta, error) {
	var d delta
	if json.Unmarshal(b, &d) != nil || d.ID == 0 || d.ID > 1e12 || d.Version == 0 || d.Version > 1e12 || d.Deleted != (d.Row == nil) || d.Row != nil && (!d.Row.valid() || d.Row.ID != d.ID || d.Row.Version != d.Version) {
		return d, kv.ErrCorrupt
	}
	return d, nil
}
func current(tx *bolt.Tx, id uint64, h head) (Fact, error) {
	if h.Deleted {
		return Fact{}, kv.ErrNotFound
	}
	if h.Segment == 0 {
		d, e := readDelta(tx.Bucket(deltaBucket).Get(idBytes(id)))
		if e != nil || d.Row == nil {
			return Fact{}, kv.ErrCorrupt
		}
		if d.Row.ID != id || d.Row.Version != h.Version || d.Row.Tenant != h.Tenant {
			return Fact{}, kv.ErrCorrupt
		}
		return *d.Row, nil
	}
	s, e := readCatalog(tx.Bucket(segmentBucket).Get(idBytes(h.Segment)))
	if e != nil || h.Group >= len(s.Groups) || h.Offset >= s.Groups[h.Group].Rows {
		return Fact{}, kv.ErrCorrupt
	}
	values := map[Column][]Value{}
	for _, c := range columns {
		v, e := s.column(h.Group, c)
		if e != nil {
			return Fact{}, e
		}
		values[c] = v.Values
	}
	r, e := factFrom(values, h.Offset)
	if e != nil || r.ID != id || r.Version != h.Version || r.Tenant != h.Tenant {
		return Fact{}, kv.ErrCorrupt
	}
	return r, nil
}
func Open(path string, o Options) (*DB, error) {
	if path == "" || o.Encoding != "" && o.Encoding != Auto && o.Encoding != RLE && o.Encoding != Bitmap {
		return nil, kv.ErrInvalid
	}
	b, e := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoSync: false})
	if e != nil {
		return nil, e
	}
	d := &DB{disk: b, path: path, options: o}
	e = b.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(metaBucket) != nil {
			m, e := decodeMeta(tx)
			if e != nil {
				return e
			}
			if o.Encoding != "" && o.Encoding != m.Encoding {
				return kv.ErrInvalid
			}
			for _, name := range [][]byte{headBucket, deltaBucket, segmentBucket, cubeBucket} {
				if tx.Bucket(name) == nil {
					return kv.ErrCorrupt
				}
			}
			return nil
		}
		if k, _ := tx.Cursor().First(); k != nil {
			return kv.ErrCorrupt
		}
		for _, name := range [][]byte{metaBucket, headBucket, deltaBucket, segmentBucket, cubeBucket} {
			if _, e := tx.CreateBucket(name); e != nil {
				return e
			}
		}
		encoding := o.Encoding
		if encoding == "" {
			encoding = Auto
		}
		return saveMeta(tx, metadata{Format: 1, Encoding: encoding, NextSegment: 1})
	})
	if e == nil {
		var dir *os.File
		dir, e = os.Open(filepath.Dir(path))
		if e == nil {
			e = errors.Join(dir.Sync(), dir.Close())
		}
	}
	if e == nil {
		e = b.View(checkStore)
	}
	if e != nil {
		b.Close()
		return nil, e
	}
	return d, nil
}
func (d *DB) ready() error {
	if d.closed {
		return kv.ErrClosed
	}
	return d.bad
}
func (d *DB) hit(stage string) error {
	if d.options.Failpoint != nil {
		return d.options.Failpoint(stage)
	}
	return nil
}
func (d *DB) update(stage string, f func(*bolt.Tx) error) error {
	commit := false
	e := d.disk.Update(func(tx *bolt.Tx) error {
		if e := f(tx); e != nil {
			return e
		}
		if e := d.hit(stage + ".ready"); e != nil {
			return e
		}
		commit = true
		return nil
	})
	if e != nil {
		if commit {
			d.bad = errors.Join(kv.ErrRecovery, e)
			return d.bad
		}
		return e
	}
	if e = d.hit(stage + ".committed"); e != nil {
		d.bad = errors.Join(kv.ErrRecovery, e)
		return d.bad
	}
	return nil
}
func (d *DB) Put(r Fact) error {
	if !r.valid() {
		return kv.ErrInvalid
	}
	return d.ingest(r.Tenant, r.ID, r.Version, &r)
}
func (d *DB) Delete(tenant string, id, version uint64) error {
	if !validName(tenant) || id == 0 || id > 1e12 || version == 0 || version > 1e12 {
		return kv.ErrInvalid
	}
	return d.ingest(tenant, id, version, nil)
}
func (d *DB) ingest(tenant string, id, version uint64, r *Fact) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	return d.update("ingest", func(tx *bolt.Tx) error {
		m, e := decodeMeta(tx)
		if e != nil {
			return e
		}
		if m.State == ^uint64(0) || m.Publication == ^uint64(0) {
			return kv.ErrLimit
		}
		h, exists, e := readHead(tx, id)
		if e != nil {
			return e
		}
		if exists && (version <= h.Version || tenant != h.Tenant) {
			return ErrVersion
		}
		if !exists {
			if m.Heads >= MaxIDs {
				return kv.ErrLimit
			}
			m.Heads++
		}
		if tx.Bucket(deltaBucket).Get(idBytes(id)) == nil {
			if m.Delta >= MaxDelta {
				return kv.ErrLimit
			}
			m.Delta++
		}
		if exists && !h.Deleted {
			old, e := current(tx, id, h)
			if e != nil {
				return e
			}
			if e = adjustCube(tx, old, -1); e != nil {
				return e
			}
			m.Live--
		}
		if r != nil {
			if e = adjustCube(tx, *r, 1); e != nil {
				return e
			}
			m.Live++
		}
		b, _ := json.Marshal(delta{ID: id, Version: version, Deleted: r == nil, Row: r})
		if e = tx.Bucket(deltaBucket).Put(idBytes(id), b); e != nil {
			return e
		}
		if e = saveHead(tx, id, head{Tenant: tenant, Version: version, Deleted: r == nil}); e != nil {
			return e
		}
		m.State++
		m.CubeState = m.State
		m.Publication++
		return saveMeta(tx, m)
	})
}
func (d *DB) Get(tenant string, id uint64) (Fact, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.ready(); e != nil {
		return Fact{}, e
	}
	if !validName(tenant) || id == 0 || id > 1e12 {
		return Fact{}, kv.ErrInvalid
	}
	var r Fact
	e := d.disk.View(func(tx *bolt.Tx) error {
		h, ok, e := readHead(tx, id)
		if e != nil {
			return e
		}
		if !ok || h.Tenant != tenant {
			return kv.ErrNotFound
		}
		r, e = current(tx, id, h)
		return e
	})
	return r, e
}
func clear(b *bolt.Bucket) error {
	var keys [][]byte
	e := b.ForEach(func(k, _ []byte) error { keys = append(keys, append([]byte(nil), k...)); return nil })
	if e != nil {
		return e
	}
	for _, k := range keys {
		if e = b.Delete(k); e != nil {
			return e
		}
	}
	return nil
}
func publish(tx *bolt.Tx, rows []Fact, m *metadata) error {
	if len(m.Segments)+(len(rows)+SegmentRows-1)/SegmentRows > MaxSegments {
		return kv.ErrLimit
	}
	sort.Slice(rows, func(i, j int) bool { return factLess(rows[i], rows[j]) })
	for start := 0; start < len(rows); start += SegmentRows {
		if m.NextSegment == ^uint64(0) {
			return kv.ErrLimit
		}
		id := m.NextSegment
		m.NextSegment++
		b, sorted, e := encodeSegment(rows[start:min(start+SegmentRows, len(rows))], m.Encoding)
		if e != nil {
			return e
		}
		if e = tx.Bucket(segmentBucket).Put(idBytes(id), b); e != nil {
			return e
		}
		m.Segments = append(m.Segments, id)
		for i, r := range sorted {
			if e = saveHead(tx, r.ID, head{Tenant: r.Tenant, Version: r.Version, Segment: id, Group: i / GroupRows, Offset: i % GroupRows}); e != nil {
				return e
			}
		}
	}
	return nil
}
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	return d.update("flush", func(tx *bolt.Tx) error {
		m, e := decodeMeta(tx)
		if e != nil {
			return e
		}
		if m.Publication == ^uint64(0) {
			return kv.ErrLimit
		}
		var rows []Fact
		if e = tx.Bucket(deltaBucket).ForEach(func(_, v []byte) error {
			r, e := readDelta(v)
			if e != nil {
				return e
			}
			if r.Row != nil {
				rows = append(rows, *r.Row)
			}
			return nil
		}); e != nil {
			return e
		}
		if e = publish(tx, rows, &m); e != nil {
			return e
		}
		if e = clear(tx.Bucket(deltaBucket)); e != nil {
			return e
		}
		m.Delta = 0
		m.Publication++
		return saveMeta(tx, m)
	})
}
func liveRows(tx *bolt.Tx) ([]Fact, error) {
	var rows []Fact
	cache := map[uint64][]Fact{}
	e := tx.Bucket(headBucket).ForEach(func(k, v []byte) error {
		if len(k) != 8 {
			return kv.ErrCorrupt
		}
		id := binary.BigEndian.Uint64(k)
		h, _, e := readHead(tx, id)
		if e != nil {
			return e
		}
		if h.Deleted {
			return nil
		}
		var r Fact
		if h.Segment == 0 {
			r, e = current(tx, id, h)
		} else {
			values, ok := cache[h.Segment]
			if !ok {
				values, e = validateSegment(tx.Bucket(segmentBucket).Get(idBytes(h.Segment)))
				if e != nil {
					return e
				}
				cache[h.Segment] = values
			}
			pos := h.Group*GroupRows + h.Offset
			if pos < 0 || pos >= len(values) {
				return kv.ErrCorrupt
			}
			r = values[pos]
			if r.ID != id || r.Version != h.Version || r.Tenant != h.Tenant {
				return kv.ErrCorrupt
			}
		}
		if e != nil {
			return e
		}
		rows = append(rows, r)
		return nil
	})
	return rows, e
}
func (d *DB) Compact() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	return d.update("compact", func(tx *bolt.Tx) error {
		m, e := decodeMeta(tx)
		if e != nil {
			return e
		}
		if m.Publication == ^uint64(0) {
			return kv.ErrLimit
		}
		rows, e := liveRows(tx)
		if e != nil {
			return e
		}
		if e = clear(tx.Bucket(segmentBucket)); e != nil {
			return e
		}
		m.Segments = nil
		if e = publish(tx, rows, &m); e != nil {
			return e
		}
		if e = clear(tx.Bucket(deltaBucket)); e != nil {
			return e
		}
		m.Delta = 0
		m.Publication++
		return saveMeta(tx, m)
	})
}
func (d *DB) RebuildCube() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.ready(); e != nil {
		return e
	}
	return d.update("cube", func(tx *bolt.Tx) error {
		m, e := decodeMeta(tx)
		if e != nil {
			return e
		}
		if m.Publication == ^uint64(0) {
			return kv.ErrLimit
		}
		rows, e := liveRows(tx)
		if e != nil {
			return e
		}
		cells, e := fullCube(rows)
		if e != nil {
			return e
		}
		if e = clear(tx.Bucket(cubeBucket)); e != nil {
			return e
		}
		for k, a := range cells {
			b, _ := json.Marshal(a)
			if e = tx.Bucket(cubeBucket).Put([]byte(k), b); e != nil {
				return e
			}
		}
		m.CubeState = m.State
		m.Publication++
		return saveMeta(tx, m)
	})
}
func (d *DB) Materialized(tenant string, grouping uint8) (CubeResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var result CubeResult
	if e := d.ready(); e != nil {
		return result, e
	}
	if !validName(tenant) || grouping > 7 {
		return result, kv.ErrInvalid
	}
	e := d.disk.View(func(tx *bolt.Tx) error {
		m, e := decodeMeta(tx)
		if e != nil {
			return e
		}
		result.StateVersion = m.State
		return tx.Bucket(cubeBucket).ForEach(func(k, v []byte) error {
			var key CubeKey
			var a Aggregate
			if json.Unmarshal(k, &key) != nil || json.Unmarshal(v, &a) != nil {
				return kv.ErrCorrupt
			}
			if key.Tenant == tenant && key.Grouping == grouping {
				result.Cells = append(result.Cells, Cell{key, a})
			}
			return nil
		})
	})
	sortCells(result.Cells)
	return result, e
}
func checkStore(tx *bolt.Tx) error {
	m, e := decodeMeta(tx)
	if e != nil {
		return e
	}
	listed := map[uint64]bool{}
	for _, id := range m.Segments {
		listed[id] = true
	}
	segments := 0
	if e = tx.Bucket(segmentBucket).ForEach(func(k, v []byte) error {
		if len(k) != 8 || !listed[binary.BigEndian.Uint64(k)] {
			return kv.ErrCorrupt
		}
		segments++
		rows, e := validateSegment(v)
		if e != nil {
			return e
		}
		for _, r := range rows {
			h, ok, e := readHead(tx, r.ID)
			if e != nil || !ok || h.Tenant != r.Tenant || h.Version < r.Version || h.Deleted && h.Version == r.Version {
				return kv.ErrCorrupt
			}
		}
		return nil
	}); e != nil {
		return e
	}
	if segments != len(listed) {
		return kv.ErrCorrupt
	}
	count, deltaCount, liveCount := 0, 0, 0
	if e = tx.Bucket(headBucket).ForEach(func(k, _ []byte) error {
		if len(k) != 8 {
			return kv.ErrCorrupt
		}
		id := binary.BigEndian.Uint64(k)
		if id == 0 || id > 1e12 {
			return kv.ErrCorrupt
		}
		h, _, e := readHead(tx, id)
		if e != nil {
			return e
		}
		if h.Segment != 0 && !listed[h.Segment] {
			return kv.ErrCorrupt
		}
		if !h.Deleted {
			liveCount++
		}
		count++
		return nil
	}); e != nil {
		return e
	}
	if count != m.Heads || liveCount != m.Live {
		return kv.ErrCorrupt
	}
	if e = tx.Bucket(deltaBucket).ForEach(func(k, v []byte) error {
		if len(k) != 8 {
			return kv.ErrCorrupt
		}
		r, e := readDelta(v)
		if e != nil || r.ID != binary.BigEndian.Uint64(k) {
			return kv.ErrCorrupt
		}
		h, ok, e := readHead(tx, r.ID)
		if e != nil || !ok || h.Segment != 0 || h.Version != r.Version || h.Deleted != r.Deleted {
			return kv.ErrCorrupt
		}
		deltaCount++
		return nil
	}); e != nil {
		return e
	}
	if deltaCount != m.Delta {
		return kv.ErrCorrupt
	}
	rows, e := liveRows(tx)
	if e != nil {
		return e
	}
	if len(rows) != m.Live {
		return kv.ErrCorrupt
	}
	expected, e := fullCube(rows)
	if e != nil {
		return e
	}
	actual := map[string]Aggregate{}
	if e = tx.Bucket(cubeBucket).ForEach(func(k, v []byte) error {
		var a Aggregate
		if json.Unmarshal(v, &a) != nil {
			return kv.ErrCorrupt
		}
		actual[string(k)] = a
		return nil
	}); e != nil {
		return e
	}
	if !reflect.DeepEqual(actual, expected) {
		return kv.ErrCorrupt
	}
	return nil
}
func (d *DB) Check() error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.ready(); e != nil {
		return e
	}
	return d.disk.View(checkStore)
}
func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var s Stats
	if e := d.ready(); e != nil {
		return s, e
	}
	e := d.disk.View(func(tx *bolt.Tx) error {
		m, e := decodeMeta(tx)
		if e != nil {
			return e
		}
		s = Stats{StateVersion: m.State, Publication: m.Publication, Heads: m.Heads, Live: m.Live, Delta: m.Delta, Segments: len(m.Segments)}
		if e = tx.Bucket(segmentBucket).ForEach(func(_, v []byte) error { s.EncodedBytes += len(v); return nil }); e != nil {
			return e
		}
		if e = tx.Bucket(deltaBucket).ForEach(func(_, v []byte) error { s.DeltaBytes += len(v); return nil }); e != nil {
			return e
		}
		return tx.Bucket(cubeBucket).ForEach(func(_, _ []byte) error { s.CubeCells++; return nil })
	})
	if e != nil {
		return s, e
	}
	info, e := os.Stat(d.path)
	if e == nil {
		s.FileBytes = info.Size()
	}
	return s, e
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.disk.Close()
}
