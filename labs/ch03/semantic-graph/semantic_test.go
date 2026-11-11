package semantic

import (
	"context"
	"errors"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

func TestThreeRepresentationsAcrossMutations(t *testing.T) {
	r, e := Run(context.Background(), 32, 1)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Samples) != 126 || r.Triples == 0 {
		t.Fatalf("incomplete report %+v", r)
	}
	if len(r.Answers["isolated-star"]) != 1 || len(r.Answers["isolated-plus"]) != 0 || len(r.Answers["missing-star"]) != 0 {
		t.Fatal("star/plus semantics")
	}
	found := false
	for _, v := range r.Answers["cycle-plus"] {
		found = found || v.ID == "wh-000"
	}
	if !found {
		t.Fatal("cycle must reach its own start through plus")
	}
}

func TestConcurrentQueriesObserveCompleteReplacement(t *testing.T) {
	ctx := context.Background()
	initial, _ := graph.Synthetic(32)
	next := graph.Updated(initial)
	q := Cases(32)[0].Query
	oldAnswer, _ := Oracle(ctx, initial, q)
	newAnswer, _ := Oracle(ctx, next, q)
	g, _ := NewGo(initial)
	r, _ := NewTriples(initial)
	d, _ := NewDatalog(initial)
	for _, b := range []Backend{g, r, d} {
		var wg sync.WaitGroup
		errors := make(chan error, 3)
		for worker := 0; worker < 2; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 8; i++ {
					a, e := b.Query(ctx, q)
					if e != nil {
						errors <- e
						return
					}
					if !reflect.DeepEqual(a, oldAnswer) && !reflect.DeepEqual(a, newAnswer) {
						errors <- ErrInvalid
						return
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 6; i++ {
				f := initial
				if i%2 == 0 {
					f = next
				}
				if e := b.Replace(ctx, f); e != nil {
					errors <- e
					return
				}
			}
		}()
		wg.Wait()
		close(errors)
		for e := range errors {
			t.Errorf("%T replacement: %v", b, e)
		}
		b.Close(ctx)
	}
}

func TestSeededQueriesAndCancellation(t *testing.T) {
	ctx := context.Background()
	f, _ := graph.Synthetic(64)
	g, _ := NewGo(f)
	tr, _ := NewTriples(f)
	d, _ := NewDatalog(f)
	bs := []Backend{g, tr, d}
	for _, b := range bs {
		defer b.Close(ctx)
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 24; i++ {
		q := Query{Tenant: []string{"acme", "globex"}[rng.Intn(2)], Start: []string{"wh-000", "wh-003", "supplier-0", "missing"}[rng.Intn(4)], Kind: []string{"all", "route", "supplies"}[rng.Intn(3)], Direction: []string{"in", "out"}[rng.Intn(2)], MinCapacity: int64(rng.Intn(102)), Reflexive: rng.Intn(2) == 1}
		want, e := Oracle(ctx, f, q)
		if e != nil {
			t.Fatal(e)
		}
		for _, b := range bs {
			got, e := b.Query(ctx, q)
			if e != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%T %+v: %v", b, q, e)
			}
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, b := range bs {
		if _, e := b.Query(canceled, Query{Tenant: "missing", Start: "missing"}); !errors.Is(e, context.Canceled) {
			t.Fatalf("%T: %v", b, e)
		}
		if e := b.Replace(canceled, f); !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	}
}

func TestReplacementCopiesInputsAndRejectsInvalid(t *testing.T) {
	ctx := context.Background()
	f, _ := graph.Synthetic(32)
	want, _ := Oracle(ctx, f, Cases(32)[0].Query)
	g, _ := NewGo(f)
	tr, _ := NewTriples(f)
	d, _ := NewDatalog(f)
	for _, b := range []Backend{g, tr, d} {
		t.Cleanup(func() { b.Close(ctx) })
	}
	f.Nodes[0].Properties["name"] = "external mutation"
	for _, b := range []Backend{g, tr, d} {
		a, e := b.Query(ctx, Cases(32)[0].Query)
		if e != nil || !reflect.DeepEqual(a, want) {
			t.Fatalf("%T input alias %v", b, e)
		}
	}
	for _, q := range []Query{{Tenant: "acme", Start: "x", Kind: "bogus"}, {Tenant: "acme", Start: "x", MinCapacity: -1}, {Tenant: "bad tenant", Start: "x"}} {
		if _, e := Normalize(q); e == nil {
			t.Fatal(q)
		}
	}
}
