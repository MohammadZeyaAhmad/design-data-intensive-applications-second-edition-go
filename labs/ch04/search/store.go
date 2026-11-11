package search

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
	bolt "go.etcd.io/bbolt"
)

type Options struct {
	Config    *Config
	Failpoint func(string) error
}
type record struct {
	Document Document
	Deleted  bool
}
type metadata struct {
	Format     int
	Config     Config
	Generation uint64
	Records    int
}
type tenantState struct {
	docs         map[string]record
	vocabulary   map[string]int
	active       int
	text         *textIndex
	spatial      *kdNode
	graph        *graph
	spatialBuilt bool
}
type Store struct {
	mu      sync.Mutex
	disk    *bolt.DB
	config  Config
	tenants map[string]*tenantState
	options Options
	closed  bool
	bad     error
}

var documentsBucket = []byte("documents")
var metadataBucket = []byte("metadata")

func recordKey(tenant, id string) []byte { return []byte(tenant + "\x00" + id) }
func Open(path string, o Options) (*Store, error) {
	if path == "" || o.Config != nil && !o.Config.valid() {
		return nil, ErrInvalid
	}
	b, e := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoSync: false})
	if e != nil {
		return nil, e
	}
	s := &Store{disk: b, options: o, tenants: map[string]*tenantState{}}
	e = b.Update(func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		if m == nil {
			if k, _ := tx.Cursor().First(); k != nil {
				return kv.ErrCorrupt
			}
			var e error
			m, e = tx.CreateBucket(metadataBucket)
			if e != nil {
				return e
			}
			if _, e = tx.CreateBucket(documentsBucket); e != nil {
				return e
			}
			c := DefaultConfig()
			if o.Config != nil {
				c = *o.Config
			}
			v, _ := json.Marshal(metadata{Format: 1, Config: c})
			return m.Put([]byte("state"), v)
		}
		if tx.Bucket(documentsBucket) == nil {
			return kv.ErrCorrupt
		}
		return nil
	})
	if e == nil {
		e = b.View(func(tx *bolt.Tx) error {
			var m metadata
			if json.Unmarshal(tx.Bucket(metadataBucket).Get([]byte("state")), &m) != nil || m.Format != 1 || !m.Config.valid() || m.Records < 0 || m.Records > MaxDocuments {
				return kv.ErrCorrupt
			}
			if o.Config != nil && !reflect.DeepEqual(*o.Config, m.Config) {
				return ErrModel
			}
			s.config = m.Config
			count := 0
			if e := tx.Bucket(documentsBucket).ForEach(func(k, v []byte) error {
				var r record
				if json.Unmarshal(v, &r) != nil || string(k) != string(recordKey(r.Document.Tenant, r.Document.ID)) || !namePattern.MatchString(r.Document.Tenant) || !namePattern.MatchString(r.Document.ID) || r.Document.Version == 0 || r.Document.Version > 1e12 {
					return kv.ErrCorrupt
				}
				if !r.Deleted {
					if e := r.Document.valid(s.config); e != nil {
						return kv.ErrCorrupt
					}
				}
				t := s.tenant(r.Document.Tenant)
				t.docs[r.Document.ID] = r
				if !r.Deleted {
					t.active++
					terms, _ := documentTerms(r.Document)
					for term := range terms {
						t.vocabulary[term]++
					}
					if len(t.vocabulary) > MaxTerms {
						return kv.ErrCorrupt
					}
				}
				count++
				return nil
			}); e != nil {
				return e
			}
			if count != m.Records {
				return kv.ErrCorrupt
			}
			return nil
		})
	}
	if e == nil {
		var dir *os.File
		dir, e = os.Open(filepath.Dir(path))
		if e == nil {
			e = errors.Join(dir.Sync(), dir.Close())
		}
	}
	if e != nil {
		b.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) tenant(name string) *tenantState {
	t := s.tenants[name]
	if t == nil {
		t = &tenantState{docs: map[string]record{}, vocabulary: map[string]int{}}
		s.tenants[name] = t
	}
	return t
}
func (s *Store) readTenant(name string) *tenantState {
	t := s.tenants[name]
	if t == nil {
		return &tenantState{docs: map[string]record{}, vocabulary: map[string]int{}}
	}
	return t
}
func (t *tenantState) live() []Document {
	var rows []Document
	for _, r := range t.docs {
		if !r.Deleted {
			rows = append(rows, r.Document)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}
func (s *Store) ready() error {
	if s.closed {
		return kv.ErrClosed
	}
	return s.bad
}
func (s *Store) hit(stage string) error {
	if s.options.Failpoint != nil {
		return s.options.Failpoint(stage)
	}
	return nil
}
func (s *Store) Put(d Document) error {
	if e := d.valid(s.config); e != nil {
		return e
	}
	return s.write(record{Document: clone(d)})
}
func (s *Store) Delete(tenant, id string, version uint64) error {
	if !namePattern.MatchString(tenant) || !namePattern.MatchString(id) || version == 0 || version > 1e12 {
		return ErrInvalid
	}
	return s.write(record{Document: Document{Tenant: tenant, ID: id, Version: version}, Deleted: true})
}
func (s *Store) write(r record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.ready(); e != nil {
		return e
	}
	t := s.readTenant(r.Document.Tenant)
	if old, ok := t.docs[r.Document.ID]; ok && r.Document.Version <= old.Document.Version {
		return ErrVersion
	}
	oldTerms := map[string][]int{}
	if old, ok := t.docs[r.Document.ID]; ok && !old.Deleted {
		oldTerms, _ = documentTerms(old.Document)
	}
	newTerms := map[string][]int{}
	if !r.Deleted {
		newTerms, _ = documentTerms(r.Document)
	}
	termCount := len(t.vocabulary)
	for term := range oldTerms {
		if t.vocabulary[term] == 1 && newTerms[term] == nil {
			termCount--
		}
	}
	for term := range newTerms {
		if t.vocabulary[term] == 0 {
			termCount++
		}
	}
	if termCount > MaxTerms {
		return kv.ErrLimit
	}
	body, _ := json.Marshal(r)
	committing := false
	e := s.disk.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		var m metadata
		if json.Unmarshal(b.Get([]byte("state")), &m) != nil {
			return kv.ErrCorrupt
		}
		if m.Generation == ^uint64(0) {
			return kv.ErrLimit
		}
		key := recordKey(r.Document.Tenant, r.Document.ID)
		if tx.Bucket(documentsBucket).Get(key) == nil {
			if m.Records >= MaxDocuments {
				return kv.ErrLimit
			}
			m.Records++
		}
		m.Generation++
		if e := tx.Bucket(documentsBucket).Put(key, body); e != nil {
			return e
		}
		v, _ := json.Marshal(m)
		if e := b.Put([]byte("state"), v); e != nil {
			return e
		}
		if e := s.hit("document.ready"); e != nil {
			return e
		}
		committing = true
		return nil
	})
	if e != nil {
		if committing {
			s.bad = errors.Join(kv.ErrRecovery, e)
			return s.bad
		}
		return e
	}
	if e = s.hit("document.committed"); e != nil {
		s.bad = errors.Join(kv.ErrRecovery, e)
		return s.bad
	}
	if old, ok := t.docs[r.Document.ID]; ok && !old.Deleted {
		t.active--
	}
	if !r.Deleted {
		t.active++
	}
	t.docs[r.Document.ID] = r
	for term := range oldTerms {
		t.vocabulary[term]--
		if t.vocabulary[term] == 0 {
			delete(t.vocabulary, term)
		}
	}
	for term := range newTerms {
		t.vocabulary[term]++
	}
	s.tenants[r.Document.Tenant] = t
	t.text = nil
	t.spatial = nil
	t.spatialBuilt = false
	t.graph = nil
	return nil
}
func (s *Store) Text(q TextQuery) (TextResult, error) {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var r TextResult
	if e := s.ready(); e != nil {
		return r, e
	}
	if !namePattern.MatchString(q.Tenant) {
		return r, ErrInvalid
	}
	t := s.readTenant(q.Tenant)
	if t.text == nil {
		x, e := buildText(t.live())
		if e != nil {
			return r, e
		}
		t.text = x
	}
	r, e := t.text.query(q)
	r.Work.WallNanos = time.Since(start).Nanoseconds()
	return r, e
}
func (t *tenantState) spatialIndex() {
	if t.spatialBuilt {
		return
	}
	var points []geoDoc
	for _, d := range t.live() {
		if d.Point != nil {
			points = append(points, geoDoc{d.ID, d.Version, *d.Point})
		}
	}
	t.spatial = kdBuild(points, 0)
	t.spatialBuilt = true
}
func (s *Store) Box(tenant string, box Box, limit int) (SpatialResult, error) {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var r SpatialResult
	if e := s.ready(); e != nil {
		return r, e
	}
	if !namePattern.MatchString(tenant) || !box.valid() || limit < 1 || limit > 100 {
		return r, ErrInvalid
	}
	t := s.readTenant(tenant)
	t.spatialIndex()
	points, work := t.spatial.boxQuery(box)
	r.Work = work
	r.Work.Documents = t.active
	for _, p := range points {
		r.Hits = append(r.Hits, Hit{ID: p.id, Version: p.version})
	}
	sort.Slice(r.Hits, func(i, j int) bool { return r.Hits[i].ID < r.Hits[j].ID })
	if len(r.Hits) > limit {
		r.Hits = r.Hits[:limit]
	}
	r.Work.WallNanos = time.Since(start).Nanoseconds()
	return r, nil
}
func (s *Store) Radius(tenant string, center Point, km float64, limit int) (SpatialResult, error) {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var r SpatialResult
	if e := s.ready(); e != nil {
		return r, e
	}
	if !namePattern.MatchString(tenant) || !center.valid() || !finite(km) || km < 0 || km > math.Pi*earthKM || limit < 1 || limit > 100 {
		return r, ErrInvalid
	}
	t := s.readTenant(tenant)
	t.spatialIndex()
	points, work := t.spatial.boxQuery(radiusBox(center, km))
	r.Work = work
	r.Work.Documents = t.active
	for _, p := range points {
		d := Haversine(center, p.p)
		r.Work.Distances++
		if d <= km+1e-9 {
			r.Hits = append(r.Hits, Hit{ID: p.id, Version: p.version, Distance: d})
		}
	}
	sort.Slice(r.Hits, func(i, j int) bool {
		if r.Hits[i].Distance != r.Hits[j].Distance {
			return r.Hits[i].Distance < r.Hits[j].Distance
		}
		return r.Hits[i].ID < r.Hits[j].ID
	})
	if len(r.Hits) > limit {
		r.Hits = r.Hits[:limit]
	}
	r.Work.WallNanos = time.Since(start).Nanoseconds()
	return r, nil
}
func (s *Store) Vector(tenant string, v []float64, k, ef int, approx bool) (VectorResult, error) {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var r VectorResult
	if e := s.ready(); e != nil {
		return r, e
	}
	if !namePattern.MatchString(tenant) || !validVector(v, s.config) || k < 1 || k > 100 || approx && (ef < k || ef > 1024) {
		return r, ErrInvalid
	}
	t := s.readTenant(tenant)
	if approx {
		if t.graph == nil {
			t.graph = buildGraph(t.live(), s.config)
		}
		r = t.graph.query(v, k, ef)
	} else {
		r = exact(t.live(), v, k, s.config)
	}
	r.Work.WallNanos = time.Since(start).Nanoseconds()
	return r, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.tenants = nil
	return s.disk.Close()
}
