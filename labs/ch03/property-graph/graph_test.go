package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func BudgetFixture() Fixture {
	f := Fixture{}
	for i := 0; i < 7; i++ {
		f.Nodes = append(f.Nodes, Node{"acme", fmt.Sprintf("n%d", i), "warehouse", Properties{"name": fmt.Sprintf("Node %d", i)}})
	}
	for i := 0; i < 6; i++ {
		for j := 0; j < 3; j++ {
			f.Edges = append(f.Edges, Edge{"acme", fmt.Sprintf("e%d-%d", i, j), fmt.Sprintf("n%d", i), fmt.Sprintf("n%d", i+1), "route", Properties{"cost": int64(1), "capacity": int64(100), "enabled": true}})
		}
	}
	return f
}
func TestRandomQuestionsAgainstIndependentOracle(t *testing.T) {
	f, e := Synthetic(64)
	if e != nil {
		t.Fatal(e)
	}
	g, e := New(f)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close(context.Background())
	rng := rand.New(rand.NewSource(303))
	for phase := 0; phase < 3; phase++ {
		if phase == 1 {
			f = Updated(f)
		} else if phase == 2 {
			f = Deleted(f)
		}
		if e = g.Replace(context.Background(), f); e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 120; i++ {
			q := Query{Tenant: []string{"acme", "globex", "missing"}[rng.Intn(3)], Start: []string{"wh-000", "wh-002", "wh-007", "wh-028", "supplier-0", "missing"}[rng.Intn(6)], Kind: []string{"all", "route", "supplies"}[rng.Intn(3)], Direction: []string{"in", "out"}[rng.Intn(2)], Hops: rng.Intn(7), MinCapacity: []int64{0, 75, 90, 101}[rng.Intn(4)]}
			mode := []Mode{Reach, Paths, Cheapest}[rng.Intn(3)]
			if mode != Reach {
				q.Target = []string{"wh-000", "wh-002", "wh-003", "wh-007", "supplier-0", "missing"}[rng.Intn(6)]
			}
			want, e := Oracle(context.Background(), f, mode, q)
			if e != nil {
				t.Fatal(e)
			}
			got, e := g.Query(context.Background(), mode, q)
			if e != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("phase %d %s %+v differs: got %+v want %+v (%v)", phase, mode, q, got, want, e)
			}
		}
	}
}
func TestPathDefinitionsCapacityAndMutations(t *testing.T) {
	f, _ := Synthetic(64)
	g, e := New(f)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close(context.Background())
	q := Query{Tenant: "acme", Start: "wh-000", Target: "wh-002", Kind: "route", Hops: 6}
	a, e := g.Query(context.Background(), Cheapest, q)
	if e != nil || len(a.Paths) != 1 || a.Paths[0].Cost != 14 || len(a.Paths[0].Edges) != 2 {
		t.Fatal("weighted route wrong", a, e)
	}
	q.Hops = 1
	a, e = g.Query(context.Background(), Cheapest, q)
	if e != nil || a.Paths[0].Cost != 35 {
		t.Fatal("hop bound ignored", a, e)
	}
	q.Start, q.Target, q.Hops = "wh-002", "wh-002", 6
	a, e = g.Query(context.Background(), Paths, q)
	if e != nil || len(a.Paths) != 1 || len(a.Paths[0].Edges) != 0 || a.Paths[0].Cost != 0 {
		t.Fatal("cycle/self-loop changed zero path", a, e)
	}
	q.Start, q.Target, q.Hops = "wh-000", "wh-001", 1
	a, e = g.Query(context.Background(), Paths, q)
	if e != nil || len(a.Paths) != 2 || a.Paths[0].Edges[0] == a.Paths[1].Edges[0] {
		t.Fatal("parallel relationships lost", a, e)
	}
	if e = g.Replace(context.Background(), Updated(f)); e != nil {
		t.Fatal(e)
	}
	q.Start, q.Target, q.Hops = "wh-000", "wh-002", 6
	a, e = g.Query(context.Background(), Cheapest, q)
	if e != nil || a.Paths[0].Cost != 3 || len(a.Paths[0].Edges) != 1 {
		t.Fatal("property replacement stale", a, e)
	}
}
func TestResultBudgetAndFixtureBounds(t *testing.T) {
	f := BudgetFixture()
	g, e := New(f)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close(context.Background())
	q := Query{Tenant: "acme", Start: "n0", Target: "n6", Kind: "route", Hops: 6}
	if _, e = g.Query(context.Background(), Paths, q); !errors.Is(e, ErrBudget) {
		t.Fatal("partial path success", e)
	}
	a, e := g.Query(context.Background(), Cheapest, q)
	if e != nil || len(a.Paths) != 1 || a.Paths[0].Cost != 6 || !reflect.DeepEqual(a.Paths[0].Edges, []string{"e0-0", "e1-0", "e2-0", "e3-0", "e4-0", "e5-0"}) {
		t.Fatal(a, e)
	}
	f.Edges = append(f.Edges, Edge{"acme", "extra-0", "n0", "n1", "route", Properties{"cost": int64(1), "capacity": int64(1), "enabled": true}}, Edge{"acme", "extra-1", "n0", "n1", "route", Properties{"cost": int64(1), "capacity": int64(1), "enabled": true}})
	if Validate(f) == nil {
		t.Fatal("unbounded degree accepted")
	}
	f, _ = Synthetic(32)
	f.Edges[0].To = "foreign-node"
	if Validate(f) == nil {
		t.Fatal("dangling reference accepted")
	}
	f, _ = Synthetic(32)
	f.Nodes[0].Properties["tenant"] = "globex"
	if Validate(f) == nil {
		t.Fatal("reserved identity property accepted")
	}
}
func TestExpandedPropertySerializationAndPositiveCosts(t *testing.T) {
	p := Properties{"name": "Hub"}
	for i := 0; i < 15; i++ {
		p[fmt.Sprintf("p%02d", i)] = strings.Repeat("<", 256)
	}
	body, e := json.Marshal(p)
	if e != nil || len(body) <= 8192 {
		t.Fatal("fixture did not expand HTML escaping", e)
	}
	var decoded Properties
	if e = json.Unmarshal(body, &decoded); e != nil || !reflect.DeepEqual(p, decoded) {
		t.Fatal("valid properties failed serialization round trip", e)
	}
	for _, cost := range []int64{-1, 0, 1000001} {
		f, _ := Synthetic(32)
		f.Edges[0].Properties["cost"] = cost
		if Validate(f) == nil {
			t.Fatal("unsupported Dijkstra cost accepted", cost)
		}
	}
	if e = json.Unmarshal([]byte{'{', '"', 'n', 'a', 'm', 'e', '"', ':', '"', 255, '"', '}'}, &decoded); e == nil {
		t.Fatal("invalid UTF-8 property accepted")
	}
}
func TestCloningCancellationAndClosedState(t *testing.T) {
	f, _ := Synthetic(32)
	saved := Clone(f)
	g, e := New(f)
	if e != nil {
		t.Fatal(e)
	}
	f.Nodes[0].Properties["name"] = "caller mutation"
	f.Edges[0].Properties["enabled"] = false
	q := Cases(32)[0].Query
	want, _ := Oracle(context.Background(), saved, Reach, q)
	got, e := g.Query(context.Background(), Reach, q)
	if e != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("retained caller map")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = g.Query(ctx, Reach, q); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e = g.Replace(ctx, saved); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	g.Close(context.Background())
	if _, e = g.Query(context.Background(), Reach, q); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}
func TestConcurrentCompleteReplacement(t *testing.T) {
	f, _ := Synthetic(32)
	updated := Updated(f)
	g, _ := New(f)
	defer g.Close(context.Background())
	q := Cases(32)[0].Query
	old, _ := Oracle(context.Background(), f, Reach, q)
	fresh, _ := Oracle(context.Background(), updated, Reach, q)
	var wg sync.WaitGroup
	fail := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if i == 0 {
					next := f
					if j%2 == 0 {
						next = updated
					}
					if e := g.Replace(context.Background(), next); e != nil {
						fail <- e
						return
					}
				} else {
					a, e := g.Query(context.Background(), Reach, q)
					if e != nil || !reflect.DeepEqual(a, old) && !reflect.DeepEqual(a, fresh) {
						fail <- fmt.Errorf("mixed graph replacement: %v", e)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	close(fail)
	for e := range fail {
		t.Fatal(e)
	}
}
func TestExecutableGraphScenario(t *testing.T) {
	r, e := RunScenario(context.Background(), 64, 1)
	if e != nil || !r.Passed || len(r.Samples) != 48 || len(r.Cases) != 16 {
		t.Fatal("incomplete scenario", e)
	}
}
func FuzzPropertiesAndBounds(f *testing.F) {
	f.Add([]byte(`{"name":"Hub","cost":1,"enabled":true}`), int64(6))
	f.Add([]byte(`{"name":"Hub","cost":1.5}`), int64(7))
	f.Add([]byte(`{"run":"foreign"}`), int64(-1))
	large := Properties{"name": "Hub"}
	for i := 0; i < 15; i++ {
		large[fmt.Sprintf("p%02d", i)] = strings.Repeat("<", 256)
	}
	encoded, _ := json.Marshal(large)
	f.Add(encoded, int64(MaxReachHops))
	f.Fuzz(func(t *testing.T, b []byte, hops int64) {
		var p Properties
		if e := json.Unmarshal(b, &p); e == nil {
			out, e := json.Marshal(p)
			if e != nil {
				t.Fatal(e)
			}
			var again Properties
			if json.Unmarshal(out, &again) != nil || !reflect.DeepEqual(p, again) {
				t.Fatal("scalar properties changed")
			}
		}
		q, e := Normalize(Reach, Query{Tenant: "acme", Start: "wh-000", Hops: int(hops)})
		if e == nil && (q.Hops < 0 || q.Hops > MaxReachHops) {
			t.Fatal("unbounded traversal")
		}
	})
}
