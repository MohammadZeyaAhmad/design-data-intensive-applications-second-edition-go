package btree

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch04/kv"
)

func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func mustOpen(t *testing.T, dir string, o Options) *DB {
	t.Helper()
	d, e := Open(dir, o)
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func key(i int) []byte { return []byte(fmt.Sprintf("key/%04d", i)) }
func expectedRows(m map[string]string, start, end []byte, limit int) []Pair {
	var keys []string
	for k := range m {
		if bytes.Compare([]byte(k), start) >= 0 && (end == nil || bytes.Compare([]byte(k), end) < 0) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var r []Pair
	for _, k := range keys {
		r = append(r, Pair{Key: []byte(k), Value: []byte(m[k])})
		if len(r) == limit {
			break
		}
	}
	return r
}
func assertMap(t *testing.T, d *DB, want map[string]string) {
	t.Helper()
	got, e := d.Scan(nil, nil, kv.MaxScan)
	must(t, e)
	expected := expectedRows(want, nil, nil, kv.MaxScan)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("map mismatch: got %v want %v", got, expected)
	}
	for k, v := range want {
		b, e := d.Get([]byte(k))
		if e != nil || string(b) != v {
			t.Fatalf("point mismatch %q got=%q err=%v", k, b, e)
		}
	}
	check, e := d.Check()
	must(t, e)
	if check.Keys != uint64(len(want)) {
		t.Fatal("key count", check, len(want))
	}
}

func TestRandomMutationsPreserveMapAndPageInvariants(t *testing.T) {
	for _, order := range []int{4, 6, 8} {
		for seed := int64(0); seed < 4; seed++ {
			t.Run(fmt.Sprintf("order-%d/seed-%d", order, seed), func(t *testing.T) {
				dir := t.TempDir()
				d := mustOpen(t, dir, Options{MaxKeys: order})
				defer func() { d.Close() }()
				model := map[string]string{}
				rng := rand.New(rand.NewSource(seed))
				for step := 0; step < 600; step++ {
					k := key(rng.Intn(100))
					v := fmt.Sprintf("stock/%d/%d", step, rng.Int63())
					switch rng.Intn(5) {
					case 0:
						must(t, d.Delete(k))
						delete(model, string(k))
					case 1, 2:
						must(t, d.Put(k, []byte(v)))
						model[string(k)] = v
					case 3:
						b, e := d.Get(k)
						want, ok := model[string(k)]
						if ok && (e != nil || string(b) != want) || !ok && !errors.Is(e, ErrNotFound) {
							t.Fatal("point oracle", string(k), e)
						}
					case 4:
						start := key(rng.Intn(70))
						end := key(70 + rng.Intn(30))
						limit := 1 + rng.Intn(12)
						got, e := d.Scan(start, end, limit)
						must(t, e)
						if !reflect.DeepEqual(got, expectedRows(model, start, end, limit)) {
							t.Fatal("range oracle")
						}
					}
					if _, e := d.Check(); e != nil {
						t.Fatalf("step %d: %v", step, e)
					}
					if step%67 == 0 {
						must(t, d.Close())
						d = mustOpen(t, dir, Options{})
						assertMap(t, d, model)
					}
				}
				assertMap(t, d, model)
			})
		}
	}
}

func TestRootGrowthCollapseAndDurableFreePageReuse(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{MaxKeys: 4})
	defer func() { d.Close() }()
	want := map[string]string{}
	for i := 0; i < 320; i++ {
		v := fmt.Sprint(i)
		must(t, d.Put(key(i), []byte(v)))
		want[string(key(i))] = v
	}
	assertMap(t, d, want)
	before, e := d.Stats()
	must(t, e)
	if before.Height < 4 || before.Counters.Splits == 0 {
		t.Fatal("tree did not grow", before)
	}
	rng := rand.New(rand.NewSource(42))
	for _, i := range rng.Perm(320) {
		must(t, d.Delete(key(i)))
		delete(want, string(key(i)))
		if _, e = d.Check(); e != nil {
			t.Fatal("delete", i, e)
		}
	}
	empty, e := d.Stats()
	must(t, e)
	if empty.Height != 1 || empty.Keys != 0 || empty.FreePages != empty.AllocatedPages-1 || empty.Counters.Merges == 0 || empty.Counters.Borrows == 0 {
		t.Fatal("tree did not collapse and free", empty)
	}
	must(t, d.Close())
	d = mustOpen(t, dir, Options{})
	for i := 0; i < 320; i++ {
		v := fmt.Sprint(i)
		must(t, d.Put(key(i), []byte(v)))
		want[string(key(i))] = v
	}
	after, e := d.Stats()
	must(t, e)
	if after.AllocatedPages != before.AllocatedPages || after.Counters.FreeReuses == 0 {
		t.Fatal("freed pages not reused", before, after)
	}
	assertMap(t, d, want)
	must(t, d.Close())
	d = mustOpen(t, dir, Options{})
	assertMap(t, d, want)
}

func TestOverwriteSizesCopiesEmptyValuesAndBounds(t *testing.T) {
	d := mustOpen(t, t.TempDir(), Options{})
	defer d.Close()
	k, v := []byte("a"), []byte("first")
	must(t, d.Put(k, v))
	k[0] = 'z'
	v[0] = 'x'
	b, e := d.Get([]byte("a"))
	must(t, e)
	if string(b) != "first" {
		t.Fatal("input aliased")
	}
	b[0] = 'y'
	for _, size := range []int{0, MaxValueBytes, 1, 200, 0} {
		value := bytes.Repeat([]byte("v"), size)
		must(t, d.Put([]byte("a"), value))
		got, e := d.Get([]byte("a"))
		if e != nil || !bytes.Equal(got, value) {
			t.Fatal("replacement", size, e)
		}
	}
	must(t, d.Put([]byte{0, 255}, nil))
	must(t, d.Put([]byte{255}, []byte("last")))
	rows, e := d.Scan([]byte{0}, kv.PrefixEnd([]byte{0}), 10)
	must(t, e)
	if len(rows) != 1 {
		t.Fatal("binary range")
	}
	if e = d.Put(nil, nil); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e = d.Put([]byte("bad"), make([]byte, MaxValueBytes+1)); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e = d.Scan([]byte("z"), []byte("a"), 2); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	before, e := d.Stats()
	must(t, e)
	must(t, d.Delete([]byte("absent")))
	after, e := d.Stats()
	must(t, e)
	if before.Generation != after.Generation {
		t.Fatal("absent delete rewrote the tree")
	}
	_, e = d.Check()
	must(t, e)
}

func TestWriterLockOrderAndConcurrentCalls(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{MaxKeys: 4})
	defer func() { d.Close() }()
	if other, e := Open(dir, Options{}); !errors.Is(e, ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				k := []byte(fmt.Sprintf("worker/%d/%03d", worker, i))
				if e := d.Put(k, []byte("v")); e != nil {
					t.Error(e)
					return
				}
				if _, e := d.Get(k); e != nil {
					t.Error(e)
				}
				if _, e := d.Scan(nil, nil, 12); e != nil {
					t.Error(e)
				}
			}
		}(worker)
	}
	wg.Wait()
	check, e := d.Check()
	must(t, e)
	if check.Keys != 360 {
		t.Fatal(check)
	}
	must(t, d.Close())
	must(t, d.Close())
	if _, e = d.Get(key(1)); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if other, e := Open(dir, Options{MaxKeys: 8}); !errors.Is(e, ErrInvalid) {
		if other != nil {
			other.Close()
		}
		t.Fatal("order changed", e)
	}
	d = mustOpen(t, dir, Options{})
	_, e = d.Check()
	must(t, e)
}

func TestRangeSeekDoesNotReadUnrelatedLeaves(t *testing.T) {
	d := mustOpen(t, t.TempDir(), Options{})
	defer d.Close()
	for i := 0; i < 500; i++ {
		must(t, d.Put(key(i), []byte("v")))
	}
	before, e := d.Stats()
	must(t, e)
	rows, e := d.Scan(key(450), key(453), 10)
	must(t, e)
	after, e := d.Stats()
	must(t, e)
	if len(rows) != 3 || after.Counters.ReadBytes-before.Counters.ReadBytes > uint64(after.Height+2)*PageSize {
		t.Fatal("range scanned unrelated leaves", len(rows), before, after)
	}
}

func TestMissingFilesAndCorruptionFailClosed(t *testing.T) {
	for _, file := range []string{"WAL", "CONTROL", "DATA", "page"} {
		t.Run(file, func(t *testing.T) {
			dir := t.TempDir()
			d := mustOpen(t, dir, Options{})
			must(t, d.Put([]byte("a"), []byte("v")))
			must(t, d.Close())
			if file == "page" {
				path := filepath.Join(dir, "DATA")
				b, e := os.ReadFile(path)
				must(t, e)
				b[PageSize+60] ^= 1
				must(t, os.WriteFile(path, b, 0600))
			} else {
				must(t, os.Remove(filepath.Join(dir, file)))
			}
			if other, e := Open(dir, Options{}); !errors.Is(e, ErrCorrupt) {
				if other != nil {
					other.Close()
				}
				t.Fatal("damage accepted", e)
			}
		})
	}
}

func TestPoisonedCommitAndTornDataAreRedone(t *testing.T) {
	for _, damage := range []string{"leaf", "metadata", "short-file"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			fail := false
			o := Options{Failpoint: func(p string) error {
				if fail && p == "wal.synced" {
					return errors.New("lost after durable WAL")
				}
				return nil
			}}
			d := mustOpen(t, dir, o)
			must(t, d.Put([]byte("a"), []byte("old")))
			fail = true
			if e := d.Put([]byte("a"), []byte("new")); !errors.Is(e, ErrRecovery) {
				t.Fatal(e)
			}
			if _, e := d.Get([]byte("a")); !errors.Is(e, ErrRecovery) {
				t.Fatal("poisoned handle usable", e)
			}
			must(t, d.Close())
			path := filepath.Join(dir, "DATA")
			b, e := os.ReadFile(path)
			must(t, e)
			switch damage {
			case "metadata":
				for i := 0; i < 100; i++ {
					b[i] = 0xff
				}
			case "leaf":
				for i := PageSize; i < PageSize+100; i++ {
					b[i] = 0xff
				}
			case "short-file":
				b = b[:PageSize+97]
			}
			must(t, os.WriteFile(path, b, 0600))
			d = mustOpen(t, dir, Options{})
			defer d.Close()
			assertMap(t, d, map[string]string{"a": "new"})
			s, e := d.Stats()
			must(t, e)
			if s.Counters.Recovered != 1 {
				t.Fatal("redo missing", s)
			}
		})
	}
}

func TestRecoveryCanBeInterruptedAndReplayedAgain(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{Failpoint: func(p string) error {
		if p == "wal.synced" {
			return errors.New("stop")
		}
		return nil
	}})
	if e := d.Put([]byte("a"), []byte("new")); !errors.Is(e, ErrRecovery) {
		t.Fatal(e)
	}
	must(t, d.Close())
	if interrupted, e := Open(dir, Options{Failpoint: func(p string) error {
		if p == "recovery.page.written" {
			return errors.New("recovery interrupted")
		}
		return nil
	}}); e == nil {
		interrupted.Close()
		t.Fatal("injection missed")
	}
	d = mustOpen(t, dir, Options{})
	defer d.Close()
	assertMap(t, d, map[string]string{"a": "new"})
}

func TestStaleAndForeignRedoCannotRollBackACommittedTree(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{Failpoint: func(p string) error {
		if p == "wal.synced" {
			return errors.New("stop")
		}
		return nil
	}})
	if e := d.Put([]byte("a"), []byte("first")); !errors.Is(e, ErrRecovery) {
		t.Fatal(e)
	}
	must(t, d.Close())
	old, e := os.ReadFile(filepath.Join(dir, "WAL"))
	must(t, e)
	d = mustOpen(t, dir, Options{})
	must(t, d.Put([]byte("a"), []byte("second")))
	must(t, d.Close())
	before, e := os.ReadFile(filepath.Join(dir, "DATA"))
	must(t, e)
	must(t, os.WriteFile(filepath.Join(dir, "WAL"), old, 0600))
	if opened, e := Open(dir, Options{}); !errors.Is(e, ErrCorrupt) {
		if opened != nil {
			opened.Close()
		}
		t.Fatal("stale log accepted", e)
	}
	after, e := os.ReadFile(filepath.Join(dir, "DATA"))
	must(t, e)
	if !bytes.Equal(before, after) {
		t.Fatal("stale recovery changed data")
	}
	otherDir := t.TempDir()
	other := mustOpen(t, otherDir, Options{})
	must(t, other.Close())
	must(t, os.WriteFile(filepath.Join(otherDir, "WAL"), old, 0600))
	if opened, e := Open(otherDir, Options{}); !errors.Is(e, ErrCorrupt) {
		if opened != nil {
			opened.Close()
		}
		t.Fatal("foreign log accepted", e)
	}
}
