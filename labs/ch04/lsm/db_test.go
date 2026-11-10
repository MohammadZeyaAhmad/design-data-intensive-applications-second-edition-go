package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func mustOpen(t *testing.T, dir string, o Options) *DB {
	t.Helper()
	d, e := Open(dir, o)
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func assertMap(t *testing.T, d *DB, want map[string]string) {
	t.Helper()
	got, e := d.Scan(nil, nil, maxEntries)
	must(t, e)
	actual := map[string]string{}
	for _, p := range got {
		actual[string(p.Key)] = string(p.Value)
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("state mismatch: got %v want %v", actual, want)
	}
	for k, v := range want {
		b, e := d.Get([]byte(k))
		if e != nil || string(b) != v {
			t.Fatalf("point read %q: %q %v", k, b, e)
		}
	}
}

func TestReferenceModelAcrossPoliciesAndReopens(t *testing.T) {
	for _, style := range []Style{Leveled, Tiered} {
		for seed := int64(0); seed < 6; seed++ {
			t.Run(fmt.Sprintf("%s/%d", style, seed), func(t *testing.T) {
				o := Options{Style: style, MemtableBytes: 512, BlockBytes: 1024, TargetTableBytes: 2048, Fanout: 2}
				if seed%2 == 1 {
					o.Durability = AsyncWrites
				}
				dir := t.TempDir()
				d := mustOpen(t, dir, o)
				defer func() { must(t, d.Close()) }()
				model := map[string]string{}
				rng := rand.New(rand.NewSource(seed))
				var seq uint64
				for n := 0; n < 350; n++ {
					k := fmt.Sprintf("tenant/%02d", rng.Intn(40))
					v := fmt.Sprintf("stock-%d-%d", n, rng.Int63())
					switch rng.Intn(5) {
					case 0:
						must(t, d.Delete([]byte(k)))
						delete(model, k)
						seq++
					case 1, 2:
						must(t, d.Put([]byte(k), []byte(v)))
						model[k] = v
						seq++
					case 3:
						got, e := d.Get([]byte(k))
						want, ok := model[k]
						if ok && (e != nil || string(got) != want) || !ok && !errors.Is(e, ErrNotFound) {
							t.Fatal("point oracle", k, string(got), e)
						}
					case 4:
						assertMap(t, d, model)
					}
					if n%37 == 0 {
						must(t, d.Flush())
					}
					if n%71 == 0 {
						must(t, d.Compact())
					}
					if n%53 == 0 {
						must(t, d.Close())
						d = mustOpen(t, dir, o)
						assertMap(t, d, model)
					}
					s, e := d.Stats()
					must(t, e)
					if s.Sequence != seq {
						t.Fatal("sequence regressed", s.Sequence, seq)
					}
				}
				must(t, d.Compact())
				assertMap(t, d, model)
				must(t, d.Close())
				d = mustOpen(t, dir, o)
				assertMap(t, d, model)
			})
		}
	}
}

func TestPartialCompactionKeepsDeleteAndSequenceWins(t *testing.T) {
	o := Options{DisableAutoCompaction: true}
	dir := t.TempDir()
	d := mustOpen(t, dir, o)
	defer func() { d.Close() }()
	must(t, d.Put([]byte("a"), []byte("old")))
	must(t, d.Flush())
	old := d.manifest.Tables[0]
	must(t, d.Delete([]byte("a")))
	must(t, d.Flush())
	must(t, d.Put([]byte("z"), []byte("last")))
	must(t, d.Flush())
	// The old value remains outside this partial merge. Its deletion must stay.
	must(t, d.compact(append([]descriptor(nil), d.manifest.Tables[1:]...), 1, false))
	if _, e := d.Get([]byte("a")); !errors.Is(e, ErrNotFound) {
		t.Fatal("delete resurrected", e)
	}
	must(t, d.Close())
	d = mustOpen(t, dir, o)
	if _, e := d.Get([]byte("a")); !errors.Is(e, ErrNotFound) {
		t.Fatal("delete lost on reopen", e)
	}
	must(t, d.Put([]byte("a"), []byte("new")))
	must(t, d.Flush())
	// Rewriting the older value gives it the newest file ID, not a newer version.
	must(t, d.compact([]descriptor{old}, 0, false))
	assertMap(t, d, map[string]string{"a": "new", "z": "last"})
	must(t, d.Delete([]byte("a")))
	must(t, d.Compact())
	assertMap(t, d, map[string]string{"z": "last"})
	for _, table := range d.tables {
		if table.index.Count != 1 {
			t.Fatal("full compaction retained obsolete versions or deletes")
		}
	}
}

func TestSingleWriterCopiesAndBinaryRanges(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{})
	if _, e := Open(dir, Options{}); !errors.Is(e, ErrLocked) {
		t.Fatal("second writer accepted", e)
	}
	k, v := []byte("a"), []byte("value")
	must(t, d.Put(k, v))
	k[0] = 'z'
	v[0] = 'X'
	b, e := d.Get([]byte("a"))
	must(t, e)
	b[0] = 'Y'
	b, e = d.Get([]byte("a"))
	if e != nil || string(b) != "value" {
		t.Fatal("borrowed memory", string(b), e)
	}
	must(t, d.Put([]byte{0, 255}, nil))
	must(t, d.Put([]byte{255}, []byte("ff")))
	got, e := d.Scan([]byte{0}, PrefixEnd([]byte{0}), 10)
	must(t, e)
	if len(got) != 1 || !bytes.Equal(got[0].Key, []byte{0, 255}) {
		t.Fatal(got)
	}
	if PrefixEnd([]byte{255}) != nil {
		t.Fatal("ff bound")
	}
	must(t, d.Flush())
	empty, e := d.Get([]byte{0, 255})
	if e != nil || len(empty) != 0 {
		t.Fatal("empty value became a delete", e)
	}
	if e = d.Put(nil, []byte("x")); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if e = d.Put(bytes.Repeat([]byte("k"), MaxKeyBytes+1), nil); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e = d.Scan([]byte("z"), []byte("a"), 10); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	must(t, d.Close())
	must(t, d.Close())
	if _, e = d.Get([]byte("a")); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	d = mustOpen(t, dir, Options{})
	must(t, d.Close())
}

func TestConcurrentAccessSerializesWithoutAliasing(t *testing.T) {
	d := mustOpen(t, t.TempDir(), Options{Durability: AsyncWrites})
	defer d.Close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := []byte(fmt.Sprintf("key/%d", i))
			for n := 0; n < 100; n++ {
				if e := d.Put(k, []byte(fmt.Sprint(n))); e != nil {
					t.Error(e)
					return
				}
				if _, e := d.Get(k); e != nil {
					t.Error(e)
					return
				}
				if _, e := d.Scan(nil, nil, 100); e != nil {
					t.Error(e)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	must(t, d.Sync())
	for i := 0; i < 8; i++ {
		b, e := d.Get([]byte(fmt.Sprintf("key/%d", i)))
		if e != nil || string(b) != "99" {
			t.Fatal(string(b), e)
		}
	}
}

func TestIncompleteWalTailIsTruncatedButCorruptionIsRejected(t *testing.T) {
	frame := encodeRecord(record{key: []byte("a"), value: []byte("second"), seq: 2})
	for cut := 1; cut < len(frame); cut++ {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			dir := t.TempDir()
			d := mustOpen(t, dir, Options{})
			must(t, d.Put([]byte("a"), []byte("first")))
			must(t, d.Close())
			path := filepath.Join(dir, "WAL")
			before, e := os.Stat(path)
			must(t, e)
			f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			must(t, e)
			_, e = f.Write(frame[:cut])
			must(t, e)
			must(t, f.Sync())
			must(t, f.Close())
			d = mustOpen(t, dir, Options{})
			assertMap(t, d, map[string]string{"a": "first"})
			must(t, d.Close())
			after, e := os.Stat(path)
			must(t, e)
			if after.Size() != before.Size() {
				t.Fatal("tail not truncated")
			}
		})
	}
	for _, kind := range []string{"body", "header", "gap"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			d := mustOpen(t, dir, Options{})
			must(t, d.Put([]byte("a"), []byte("first")))
			must(t, d.Close())
			b := clone(frame)
			switch kind {
			case "body":
				b[len(b)-1] ^= 1
			case "header":
				b[4] ^= 1
			case "gap":
				b = encodeRecord(record{key: []byte("a"), value: []byte("second"), seq: 3})
			}
			f, e := os.OpenFile(filepath.Join(dir, "WAL"), os.O_APPEND|os.O_WRONLY, 0600)
			must(t, e)
			_, e = f.Write(b)
			must(t, e)
			must(t, f.Close())
			if opened, e := Open(dir, Options{}); !errors.Is(e, ErrCorrupt) {
				if opened != nil {
					opened.Close()
				}
				t.Fatal("corruption accepted", e)
			}
		})
	}
}

func TestMissingFilesAndChecksumDamageFailClosed(t *testing.T) {
	for _, kind := range []string{"WAL", "table", "manifest", "missing_table"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			d := mustOpen(t, dir, Options{})
			must(t, d.Put([]byte("a"), []byte("v")))
			must(t, d.Flush())
			name := tableName(d.manifest.Tables[0].ID)
			must(t, d.Close())
			switch kind {
			case "WAL":
				must(t, os.Remove(filepath.Join(dir, "WAL")))
			case "missing_table":
				must(t, os.Remove(filepath.Join(dir, name)))
			default:
				if kind == "manifest" {
					name = "MANIFEST"
				}
				p := filepath.Join(dir, name)
				b, e := os.ReadFile(p)
				must(t, e)
				b[len(b)/2] ^= 1
				must(t, os.WriteFile(p, b, 0600))
			}
			if opened, e := Open(dir, Options{}); !errors.Is(e, ErrCorrupt) {
				if opened != nil {
					opened.Close()
				}
				t.Fatal("damage accepted", e)
			}
		})
	}
}

func TestOrphanTablesCannotResurrectDeletedData(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{})
	must(t, d.Put([]byte("a"), []byte("old")))
	must(t, d.Flush())
	name := tableName(d.manifest.Tables[0].ID)
	old, e := os.ReadFile(filepath.Join(dir, name))
	must(t, e)
	must(t, d.Delete([]byte("a")))
	must(t, d.Compact())
	must(t, d.Close())
	must(t, os.WriteFile(filepath.Join(dir, name), old, 0600))
	must(t, os.WriteFile(filepath.Join(dir, "MANIFEST.tmp"), []byte("unpublished"), 0600))
	d = mustOpen(t, dir, Options{})
	defer d.Close()
	assertMap(t, d, map[string]string{})
	if _, e = os.Stat(filepath.Join(dir, name)); !os.IsNotExist(e) {
		t.Fatal("orphan retained", e)
	}
}

func TestBloomNoFalseNegativesAndBoundedFalsePositives(t *testing.T) {
	b := newBloom(5000)
	for i := 0; i < 5000; i++ {
		b.add([]byte(fmt.Sprintf("present/%d", i)))
	}
	for i := 0; i < 5000; i++ {
		if !b.mayContain([]byte(fmt.Sprintf("present/%d", i))) {
			t.Fatal("false negative", i)
		}
	}
	fp := 0
	for i := 0; i < 10000; i++ {
		if b.mayContain([]byte(fmt.Sprintf("absent/%d", i))) {
			fp++
		}
	}
	if fp > 300 {
		t.Fatal("unexpected false-positive rate for fixed fixture", fp)
	}
}

func TestSparseBlocksAndBloomAvoidUnnecessaryReads(t *testing.T) {
	d := mustOpen(t, t.TempDir(), Options{MemtableBytes: 1 << 20})
	defer d.Close()
	for i := 0; i < 500; i++ {
		must(t, d.Put([]byte(fmt.Sprintf("key/%04d", 2*i)), bytes.Repeat([]byte("v"), 128)))
	}
	must(t, d.Flush())
	before, e := d.Stats()
	must(t, e)
	for i := 0; i < 499; i++ {
		if _, e := d.Get([]byte(fmt.Sprintf("key/%04d", 2*i+1))); !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
	}
	after, e := d.Stats()
	must(t, e)
	if after.Counters.BloomSkips-before.Counters.BloomSkips < 450 {
		t.Fatal("Bloom did not filter fixture misses")
	}
	before = after
	b, e := d.Get([]byte("key/0900"))
	must(t, e)
	if len(b) != 128 {
		t.Fatal("wrong point read")
	}
	after, e = d.Stats()
	must(t, e)
	if after.Counters.ReadBytes-before.Counters.ReadBytes > 8192 {
		t.Fatal("point read ignored sparse index")
	}
	before = after
	rows, e := d.Scan([]byte("key/0900"), []byte("key/0904"), 10)
	must(t, e)
	if len(rows) != 2 {
		t.Fatal("range rows", len(rows))
	}
	after, e = d.Stats()
	must(t, e)
	if after.Counters.ReadBytes-before.Counters.ReadBytes > 8192 {
		t.Fatal("range seek ignored sparse index")
	}
}

func FuzzRecordDecoder(f *testing.F) {
	f.Add([]byte{})
	f.Add(encodeRecord(record{key: []byte("a"), value: []byte("v"), seq: 1}))
	f.Add(encodeRecord(record{key: []byte("b"), seq: 2, deleted: true}))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > maxRecordBytes+frameHeader {
			t.Skip()
		}
		r, next, e := readRecord(bytes.NewReader(b), 0, int64(len(b)))
		if e == nil {
			if next > int64(len(b)) || !validKey(r.key) || len(r.value) > MaxValueBytes {
				t.Fatal("decoder bounds")
			}
			round := encodeRecord(r)
			decoded, _, e := readRecord(bytes.NewReader(round), 0, int64(len(round)))
			if e != nil || !reflect.DeepEqual(r, decoded) {
				t.Fatal("record round trip", e)
			}
		}
	})
}

func TestPoisonedHandleRequiresReopen(t *testing.T) {
	dir := t.TempDir()
	fail := false
	o := Options{Failpoint: func(p string) error {
		if fail && p == "wal.synced" {
			return io.ErrClosedPipe
		}
		return nil
	}}
	d := mustOpen(t, dir, o)
	must(t, d.Put([]byte("a"), []byte("first")))
	fail = true
	if e := d.Put([]byte("a"), []byte("unknown")); !errors.Is(e, ErrRecovery) {
		t.Fatal(e)
	}
	if _, e := d.Get([]byte("a")); !errors.Is(e, ErrRecovery) {
		t.Fatal("uncertain handle stayed usable", e)
	}
	must(t, d.Close())
	d = mustOpen(t, dir, Options{})
	defer d.Close()
	assertMap(t, d, map[string]string{"a": "unknown"})
}

func TestCompactionPoliciesProduceDifferentRunLayouts(t *testing.T) {
	counts := map[Style]int{}
	for _, style := range []Style{Tiered, Leveled} {
		d := mustOpen(t, t.TempDir(), Options{Style: style, MemtableBytes: 1024, TargetTableBytes: 2048, BlockBytes: 1024, Fanout: 2})
		for i := 0; i < 200; i++ {
			must(t, d.Put([]byte(fmt.Sprintf("key/%04d", i)), bytes.Repeat([]byte("v"), 64)))
		}
		must(t, d.Flush())
		s, e := d.Stats()
		must(t, e)
		if s.Counters.Compactions == 0 {
			t.Fatal("policy never compacted")
		}
		counts[style] = s.Tables
		must(t, validateManifest(d.manifest))
		must(t, d.Close())
	}
	if counts[Tiered] >= counts[Leveled] {
		t.Fatal("fixture did not distinguish larger tiered runs from leveled target tables", counts)
	}
}

func TestWalRolloverCompactsEvenWhenOneKeyKeepsMemtableSmall(t *testing.T) {
	fs := newCrashFS()
	d, e := openFS(fs, "db", Options{MemtableBytes: maxDataBytes, TargetTableBytes: 1 << 20, Fanout: 2})
	must(t, e)
	defer d.Close()
	v := bytes.Repeat([]byte("v"), MaxValueBytes)
	for i := 0; i < 32; i++ {
		v[0] = byte(i)
		must(t, d.Put([]byte("one-key"), v))
	}
	s, e := d.Stats()
	must(t, e)
	if s.MemtableEntries != 1 || s.Counters.Flushes < 2 || s.Counters.Compactions == 0 {
		t.Fatal("WAL pressure failed to flush/compact independently of live memory", s)
	}
	got, e := d.Get([]byte("one-key"))
	must(t, e)
	if len(got) != MaxValueBytes || got[0] != 31 {
		t.Fatal("rollover lost the newest overwrite")
	}
}

func sortedPairs(m map[string]string) []Pair {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var rows []Pair
	for _, k := range keys {
		rows = append(rows, Pair{Key: []byte(k), Value: []byte(m[k])})
	}
	return rows
}
