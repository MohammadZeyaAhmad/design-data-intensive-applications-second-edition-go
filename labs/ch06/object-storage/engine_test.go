package objectstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func testEngine(s Store) Engine {
	cache, _ := NewCache(2 * MaxObjectBytes)
	return Engine{Reader: Reader{s, cache}}
}
func TestObjectContracts(t *testing.T) {
	file, err := NewFileStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatal(err)
	}
	for name, store := range map[string]Store{"memory": NewMemory(), "file": file} {
		t.Run(name, func(t *testing.T) {
			if _, err := CheckContract(context.Background(), store); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestConcurrentPublicationAndSnapshots(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()
	initial := testEngine(store)
	first, err := initial.Publish(ctx, Command{"seed", "update", []Record{{Key: "acme/east/BOOK", Value: 10}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = initial.Reader.Pin(ctx, "before", first.Ref); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := testEngine(store)
			e.Hook = func(phase string) error {
				if phase == "before_head" {
					ready <- struct{}{}
					<-release
				}
				return nil
			}
			_, err := e.Publish(ctx, Command{fmt.Sprint("writer-", i), "update", []Record{{Key: fmt.Sprintf("acme/east/SKU%d", i), Value: int64(i + 1)}}})
			results <- err
		}()
	}
	<-ready
	<-ready
	close(release)
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
		if err == nil {
			wins++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatal("same-generation overwrite was silent")
	}
	for i := range 2 {
		if _, err = initial.Publish(ctx, Command{fmt.Sprint("writer-", i), "update", []Record{{Key: fmt.Sprintf("acme/east/SKU%d", i), Value: int64(i + 1)}}}); err != nil {
			t.Fatal(err)
		}
	}
	now, err := initial.Reader.Current(ctx)
	if err != nil || now.Ref.Generation != 3 || len(now.Rows) != 3 {
		t.Fatal("explicit rebase lost a command")
	}
	if _, err = initial.Publish(ctx, Command{"seed", "update", []Record{{Key: "acme/east/BOOK", Value: 11}}}); !errors.Is(err, ErrIntent) {
		t.Fatal("intent replacement accepted")
	}
	if _, err = initial.Publish(ctx, Command{"compact", "compact", nil}); err != nil {
		t.Fatal(err)
	}
	old, err := initial.Reader.Pinned(ctx, "before")
	if err != nil || old.Ref != first.Ref || len(old.Rows) != 1 || old.Rows[0].Value != 10 {
		t.Fatal("pinned snapshot changed")
	}
	if err = initial.Reader.Pin(ctx, "before", now.Ref); !errors.Is(err, ErrConflict) {
		t.Fatal("snapshot name silently rebound")
	}
}

func TestSnapshotPinCannotPublishAnOrphanManifest(t *testing.T) {
	ctx := context.Background()
	store := NewMemory()
	e := testEngine(store)
	seed, err := e.Publish(ctx, Command{"seed", "update", []Record{{Key: "acme/east/BOOK", Value: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	e.Hook = func(phase string) error {
		if phase == "manifest" {
			return errors.New("interrupted before publication")
		}
		return nil
	}
	command := Command{"pending", "update", []Record{{Key: "acme/east/BOOK", Value: 2}}}
	if _, err = e.Publish(ctx, command); err == nil {
		t.Fatal("scheduled interruption missing")
	}
	keys, err := store.List(ctx, "manifests/")
	if err != nil {
		t.Fatal(err)
	}
	var orphan Ref
	for _, key := range keys {
		if key == seed.Ref.Key {
			continue
		}
		object, err := store.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		var m Manifest
		if strictJSON(object.Body, &m) != nil {
			t.Fatal("bad fixture manifest")
		}
		orphan = Ref{m.Generation, key, sum(object.Body)}
	}
	if orphan.Generation != 2 {
		t.Fatal("orphan manifest not found")
	}
	if err = e.Reader.Pin(ctx, "unpublished", orphan); !errors.Is(err, ErrConflict) {
		t.Fatal("pin published uncommitted data", err)
	}
	if _, err = store.Get(ctx, "pins/unpublished.json"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unpublished pin left a visibility root")
	}
	e.Hook = nil
	committed, err := e.Publish(ctx, command)
	if err != nil || committed.Ref != orphan {
		t.Fatal("retry did not reuse immutable candidate")
	}
	if err = e.Reader.Pin(ctx, "published", orphan); err != nil {
		t.Fatal(err)
	}
}

type lostReply struct {
	Store
	once   sync.Once
	before bool
}

func (s *lostReply) Put(ctx context.Context, key string, b []byte, c Condition) (string, error) {
	if key != "head.json" {
		return s.Store.Put(ctx, key, b, c)
	}
	drop := false
	s.once.Do(func() { drop = true })
	if drop && s.before {
		return "", ErrUncertain
	}
	version, err := s.Store.Put(ctx, key, b, c)
	if drop && err == nil {
		return "", ErrUncertain
	}
	return version, err
}
func TestLostPublicationReplyReconcilesSameIntent(t *testing.T) {
	for _, before := range []bool{true, false} {
		store := NewMemory()
		e := testEngine(&lostReply{Store: store, before: before})
		c := Command{"intent", "update", []Record{{Key: "acme/east/BOOK", Value: 7}}}
		result, err := e.Publish(context.Background(), c)
		if before {
			if !errors.Is(err, ErrUncertain) {
				t.Fatal("unknown outcome became success")
			}
			result, err = e.Publish(context.Background(), c)
		}
		if err != nil || result.Ref.Generation != 1 {
			t.Fatal(result, err)
		}
		retry, err := e.Publish(context.Background(), c)
		if err != nil || retry.Ref != result.Ref || !retry.Replayed {
			t.Fatal("retry created another generation")
		}
	}
}

func TestConcurrentSameIntentHasOneGenerationAndPermanentPayloadConflict(t *testing.T) {
	for _, different := range []bool{false, true} {
		store := NewMemory()
		ready := make(chan struct{}, 2)
		release := make(chan struct{})
		type outcome struct {
			publication Publication
			err         error
		}
		results := make(chan outcome, 2)
		for i := range 2 {
			go func() {
				e := testEngine(store)
				e.Hook = func(phase string) error {
					if phase == "before_head" {
						ready <- struct{}{}
						<-release
					}
					return nil
				}
				value := int64(1)
				if different {
					value += int64(i)
				}
				p, err := e.Publish(context.Background(), Command{"same-intent", "update", []Record{{Key: "acme/east/BOOK", Value: value}}})
				results <- outcome{p, err}
			}()
		}
		<-ready
		<-ready
		close(release)
		a, b := <-results, <-results
		if !different {
			if a.err != nil || b.err != nil || a.publication.Ref != b.publication.Ref || a.publication.Replayed == b.publication.Replayed {
				t.Fatal("same intent produced different commits", a, b)
			}
		} else {
			if a.err == nil {
				a, b = b, a
			}
			if !errors.Is(a.err, ErrIntent) || errors.Is(a.err, ErrUncertain) || b.err != nil {
				t.Fatal("payload conflict was retried or reported uncertain", a, b)
			}
		}
		view, err := testEngine(store).Reader.Current(context.Background())
		if err != nil || view.Ref.Generation != 1 || len(view.Rows) != 1 {
			t.Fatal("same intent advanced twice")
		}
	}
}

type missingData struct {
	Store
	corrupt bool
}

func (s missingData) Get(ctx context.Context, key string) (Object, error) {
	if len(key) > 9 && key[:9] == "segments/" {
		if !s.corrupt {
			return Object{}, ErrNotFound
		}
		o, err := s.Store.Get(ctx, key)
		if err == nil {
			o.Body[0] ^= 1
		}
		return o, err
	}
	return s.Store.Get(ctx, key)
}
func TestIncompleteOrCorruptGenerationNeverReturnsPartialRows(t *testing.T) {
	store := NewMemory()
	e := testEngine(store)
	if _, err := e.Publish(context.Background(), Command{"seed", "update", []Record{{Key: "acme/east/A", Value: 1}, {Key: "acme/east/B", Value: 1}}}); err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range []bool{false, true} {
		broken := Reader{Store: missingData{store, corrupt}}
		snapshot, err := broken.Current(context.Background())
		if !errors.Is(err, ErrCorrupt) || len(snapshot.Rows) != 0 || snapshot.Ref != (Ref{}) {
			t.Fatal("partial or unchecked generation exposed")
		}
	}
}
func TestReaderCacheKeepsOnlyImmutableVerifiedBodies(t *testing.T) {
	ctx := context.Background()
	meter := &Meter{Store: NewMemory()}
	writer := testEngine(meter)
	if _, err := writer.Publish(ctx, Command{"seed", "update", []Record{{Key: "acme/east/BOOK", Value: 1}}}); err != nil {
		t.Fatal(err)
	}
	cache, _ := NewCache(4096)
	reader := Reader{meter, cache}
	before := meter.Snapshot()
	first, err := reader.Current(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cold := meter.Snapshot()
	first.Rows[0].Value = 999
	second, err := reader.Current(ctx)
	if err != nil || second.Rows[0].Value != 1 {
		t.Fatal("mutable cache alias")
	}
	warm := meter.Snapshot()
	if cold.Gets-before.Gets != 3 || warm.Gets-cold.Gets != 1 {
		t.Fatal("head cached or immutable cache ineffective")
	}
	if _, err = writer.Publish(ctx, Command{"update", "update", []Record{{Key: "acme/east/BOOK", Value: 2}}}); err != nil {
		t.Fatal(err)
	}
	latest, err := reader.Current(ctx)
	if err != nil || latest.Rows[0].Value != 2 {
		t.Fatal("cache hid published generation")
	}
	tiny, _ := NewCache(1024)
	r := Reader{meter, tiny}
	for range 5 {
		if _, err = r.Current(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if tiny.Stats().Bytes > 1024 {
		t.Fatal("cache unbounded")
	}
}
func TestPublicationFaultSchedulesAgainstIndependentOracle(t *testing.T) {
	ctx := context.Background()
	for seed := int64(0); seed < 16; seed++ {
		rng := rand.New(rand.NewSource(seed))
		store := NewMemory()
		e := testEngine(store)
		oracle := map[string]int64{}
		for i := range 32 {
			key := fmt.Sprintf("acme/east/SKU%02d", rng.Intn(24))
			row := Record{Key: key, Value: int64(rng.Intn(100))}
			if rng.Intn(4) == 0 {
				row.Tombstone = true
				row.Value = 0
			}
			c := Command{fmt.Sprint("intent-", i), "update", []Record{row}}
			selected := []string{"segments", "manifest", "before_head", "after_head"}[rng.Intn(4)]
			e.Hook = func(phase string) error {
				if phase == selected {
					return errors.New("scheduled process interruption")
				}
				return nil
			}
			_, _ = e.Publish(ctx, c)
			e.Hook = nil
			if _, err := e.Publish(ctx, c); err != nil {
				t.Fatalf("seed %d intent %d: %v", seed, i, err)
			}
			if row.Tombstone {
				delete(oracle, key)
			} else {
				oracle[key] = row.Value
			}
			if i%7 == 6 {
				if _, err := e.Publish(ctx, Command{fmt.Sprint("compact-", i), "compact", nil}); err != nil {
					t.Fatal(err)
				}
			}
			actual, err := e.Reader.Current(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected := make([]Record, 0, len(oracle))
			for k, v := range oracle {
				expected = append(expected, Record{Key: k, Value: v})
			}
			sort.Slice(expected, func(i, j int) bool { return expected[i].Key < expected[j].Key })
			if !reflect.DeepEqual(actual.Rows, expected) {
				t.Fatalf("seed %d intent %d differs from oracle", seed, i)
			}
		}
	}
}
func FuzzBoundedObjectFormats(f *testing.F) {
	f.Add([]byte(`{"format":1,"records":[{"key":"acme/east/BOOK","value":1}]}`))
	manifest, _ := json.Marshal(Manifest{Format: 1, Generation: 1, Segments: []SegmentRef{}, Receipts: []Receipt{{ID: "seed", Hash: sum([]byte("seed")), Generation: 1}}})
	f.Add(manifest)
	f.Add([]byte("bad"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxObjectBytes {
			return
		}
		ref := SegmentRef{"segments/" + sum(b) + ".json", sum(b), len(b), 1}
		_, _ = decodeSegment(b, ref)
		_, _ = decodeManifest(b, Ref{1, "manifests/" + sum(b) + ".json", sum(b)})
		var h Head
		_ = strictJSON(b, &h)
	})
}
