package btree

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/diskmodel"
)

type modelOp struct {
	key, value []byte
	deleted    bool
}

func powerOps() []modelOp {
	var r []modelOp
	for i := 0; i < 8; i++ {
		r = append(r, modelOp{key: key(i), deleted: true})
	}
	for i := 30; i < 36; i++ {
		r = append(r, modelOp{key: key(i), value: []byte("new")})
	}
	return append(r, modelOp{key: key(10), value: []byte("updated")}, modelOp{key: key(19), deleted: true})
}
func modelAt(prefix int) map[string]string {
	m := map[string]string{}
	for i := 0; i < 20; i++ {
		m[string(key(i))] = "base"
	}
	for _, op := range powerOps()[:prefix] {
		if op.deleted {
			delete(m, string(op.key))
		} else {
			m[string(op.key)] = string(op.value)
		}
	}
	return m
}
func runModelOps(d *DB, issued, acked *int) error {
	for _, op := range powerOps() {
		*issued++
		var e error
		if op.deleted {
			e = d.Delete(op.key)
		} else {
			e = d.Put(op.key, op.value)
		}
		if e != nil {
			return e
		}
		*acked++
	}
	return nil
}
func crashDuring(f func()) (crashed bool) {
	defer func() {
		if v := recover(); v != nil {
			if _, ok := v.(diskmodel.CrashEvent); !ok {
				panic(v)
			}
			crashed = true
		}
	}()
	f()
	return
}

func TestPowerLossAtEveryPageAndWalMutation(t *testing.T) {
	variants := []struct {
		through bool
		chunk   int
	}{{false, 0}, {true, 0}, {true, 2048}}
	for _, order := range []int{4, 8} {
		for variant, mode := range variants {
			t.Run(fmt.Sprintf("order-%d/mode-%d", order, variant), func(t *testing.T) {
				base := diskmodel.New()
				d, e := openFS(base, "db", Options{MaxKeys: order})
				must(t, e)
				for i := 0; i < 20; i++ {
					must(t, d.Put(key(i), []byte("base")))
				}
				must(t, d.Close())
				base.WriteThrough = mode.through
				base.Chunk = mode.chunk
				countFS := base.Snapshot()
				d, e = openFS(countFS, "db", Options{})
				must(t, e)
				countFS.Active = true
				issued, acked := 0, 0
				must(t, runModelOps(d, &issued, &acked))
				mutations := countFS.Count
				must(t, d.Close())
				for cut := 1; cut <= mutations; cut++ {
					fs := base.Snapshot()
					db, e := openFS(fs, "db", Options{})
					must(t, e)
					fs.Active = true
					fs.Cut = cut
					issued, acked := 0, 0
					var opErr error
					if !crashDuring(func() { opErr = runModelOps(db, &issued, &acked) }) || opErr != nil {
						t.Fatalf("cut %d/%d missed: %v", cut, mutations, opErr)
					}
					fs.Crash()
					if cut%7 == 0 {
						// Crash once more during redo (or the final directory sync when
						// redo was already complete). Recovery itself must be restartable.
						fs.Active = true
						fs.Cut = 1
						fs.Count = 0
						var recoveryErr error
						if !crashDuring(func() {
							var opened *DB
							opened, recoveryErr = openFS(fs, "db", Options{})
							if opened != nil {
								opened.Close()
							}
						}) || recoveryErr != nil {
							t.Fatalf("cut %d: recovery crash missed: %v", cut, recoveryErr)
						}
						fs.Crash()
					}
					recovered, e := openFS(fs, "db", Options{})
					if e != nil {
						t.Fatalf("cut %d/%d: %v", cut, mutations, e)
					}
					s, e := recovered.Stats()
					must(t, e)
					prefix := int(s.Generation) - 20
					if prefix < acked || prefix > issued {
						t.Fatalf("cut %d: prefix %d outside [%d,%d]", cut, prefix, acked, issued)
					}
					rows, e := recovered.Scan(nil, nil, 65536)
					must(t, e)
					if !reflect.DeepEqual(rows, expectedRows(modelAt(prefix), nil, nil, 65536)) {
						t.Fatalf("cut %d: wrong map at generation %d", cut, s.Generation)
					}
					if _, e = recovered.Check(); e != nil {
						t.Fatalf("cut %d: structural invariant: %v", cut, e)
					}
					must(t, recovered.Close())
				}
				t.Logf("%d filesystem mutation cuts plus interrupted recovery schedules", mutations)
			})
		}
	}
}

func TestPowerLossDuringFirstOpenHasNoAcknowledgedDataToLose(t *testing.T) {
	for _, early := range []bool{false, true} {
		counter := diskmodel.New()
		counter.WriteThrough = early
		counter.EagerManifest = early
		counter.Active = true
		d, e := openFS(counter, "db", Options{})
		must(t, e)
		mutations := counter.Count
		must(t, d.Close())
		for cut := 1; cut <= mutations; cut++ {
			fs := diskmodel.New()
			fs.WriteThrough = early
			fs.EagerManifest = early
			fs.Active = true
			fs.Cut = cut
			if !crashDuring(func() {
				opened, e := openFS(fs, "db", Options{})
				if e != nil {
					t.Fatal(e)
				}
				if opened != nil {
					opened.Close()
				}
			}) {
				t.Fatal("initialization cut missed", early, cut)
			}
			fs.Crash()
			opened, e := openFS(fs, "db", Options{})
			if e != nil {
				t.Fatalf("initialization cut %d: %v", cut, e)
			}
			check, e := opened.Check()
			must(t, e)
			if check.Keys != 0 || check.Height != 1 {
				t.Fatal("initialization invented data", check)
			}
			must(t, opened.Close())
		}
	}
}
