package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/diskmodel"
)

type crashFS = diskmodel.FS
type powerLoss = diskmodel.CrashEvent

func newCrashFS() *crashFS { return diskmodel.New() }

var powerOperations = []record{
	{key: []byte("a"), value: []byte("new")},
	{key: []byte("b"), deleted: true},
	{key: []byte("d"), value: []byte("last")},
	{key: []byte("a"), deleted: true},
}

func powerOracle(prefix int) map[string]string {
	r := map[string]string{"a": "base", "b": "base", "c": "base"}
	for _, op := range powerOperations[:prefix] {
		if op.deleted {
			delete(r, string(op.key))
		} else {
			r[string(op.key)] = string(op.value)
		}
	}
	return r
}

type powerProgress struct{ issued, durable int }

func runPowerSchedule(d *DB, p *powerProgress) error {
	apply := func(op record) error {
		p.issued++
		var e error
		if op.deleted {
			e = d.Delete(op.key)
		} else {
			e = d.Put(op.key, op.value)
		}
		if e == nil && d.opts.Durability == SyncWrites {
			p.durable = p.issued
		}
		return e
	}
	if e := apply(powerOperations[0]); e != nil {
		return e
	}
	if e := apply(powerOperations[1]); e != nil {
		return e
	}
	if e := d.Flush(); e != nil {
		return e
	}
	p.durable = p.issued
	if e := apply(powerOperations[2]); e != nil {
		return e
	}
	if e := d.Sync(); e != nil {
		return e
	}
	p.durable = p.issued
	if e := d.Compact(); e != nil {
		return e
	}
	if e := apply(powerOperations[3]); e != nil {
		return e
	}
	if e := d.Flush(); e != nil {
		return e
	}
	p.durable = p.issued
	return d.Compact()
}
func interruptPower(f *crashFS, d *DB, p *powerProgress) (crashed bool, err error) {
	defer func() {
		if v := recover(); v != nil {
			if _, ok := v.(powerLoss); !ok {
				panic(v)
			}
			crashed = true
		}
	}()
	err = runPowerSchedule(d, p)
	return
}
func TestPowerLossAtEveryFilesystemMutation(t *testing.T) {
	variants := []struct {
		data, manifest, remove bool
		chunk                  int
	}{
		{}, {true, true, false, 0}, {true, false, true, 0}, {false, true, true, 17},
	}
	for _, style := range []Style{Tiered, Leveled} {
		for _, mode := range []Durability{SyncWrites, AsyncWrites} {
			for v, variant := range variants {
				t.Run(fmt.Sprintf("%s/%s/schedule-%d", style, mode, v), func(t *testing.T) {
					base := newCrashFS()
					o := Options{Style: style, Durability: mode, DisableAutoCompaction: true, BlockBytes: 1024, TargetTableBytes: 2048}
					d, e := openFS(base, "db", o)
					must(t, e)
					for _, k := range []string{"a", "b"} {
						must(t, d.Put([]byte(k), []byte("base")))
					}
					must(t, d.Flush())
					must(t, d.Put([]byte("c"), []byte("base")))
					must(t, d.Flush())
					must(t, d.Close())
					base.WriteThrough = variant.data
					base.EagerManifest = variant.manifest
					base.EagerRemove = variant.remove
					base.Chunk = variant.chunk
					countFS := base.Snapshot()
					d, e = openFS(countFS, "db", o)
					must(t, e)
					countFS.Active = true
					must(t, runPowerSchedule(d, &powerProgress{}))
					operations := countFS.Count
					must(t, d.Close())
					if operations < 20 {
						t.Fatal("schedule omitted publication boundaries", operations)
					}
					for cut := 1; cut <= operations; cut++ {
						fs := base.Snapshot()
						db, e := openFS(fs, "db", o)
						must(t, e)
						fs.Cut = cut
						fs.Active = true
						progress := &powerProgress{}
						crashed, e := interruptPower(fs, db, progress)
						if e != nil || !crashed {
							t.Fatalf("cut %d/%d: %v crash=%v", cut, operations, e, crashed)
						}
						fs.Crash()
						recovered, e := openFS(fs, "db", o)
						if e != nil {
							t.Fatalf("cut %d/%d: reopen %v", cut, operations, e)
						}
						s, e := recovered.Stats()
						must(t, e)
						prefix := int(s.Sequence) - 3
						if prefix < progress.durable || prefix > progress.issued {
							t.Fatalf("cut %d: recovered prefix %d outside [%d,%d]", cut, prefix, progress.durable, progress.issued)
						}
						got, e := recovered.Scan(nil, nil, maxEntries)
						must(t, e)
						if !reflect.DeepEqual(got, sortedPairs(powerOracle(prefix))) {
							t.Fatalf("cut %d: values differ at prefix %d", cut, prefix)
						}
						must(t, recovered.Close())
					}
					t.Logf("%d filesystem crash positions checked", operations)
				})
			}
		}
	}
}

func TestInitializationNeverAdoptsForeignData(t *testing.T) {
	dir := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep"), 0600))
	if d, e := Open(dir, Options{}); !errors.Is(e, ErrCorrupt) {
		if d != nil {
			d.Close()
		}
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(dir, "notes.txt"))
	must(t, e)
	if string(b) != "keep" {
		t.Fatal("unrelated data changed")
	}
}
