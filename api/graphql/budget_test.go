package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	engine "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/ast"
)

func TestBoundsVariablesFragmentsAndSchemaCostCoverage(t *testing.T) {
	for _, q := range []string{`query($n:Int=2){orders(first:$n){edges{node{id}}}}`, `query($n:Int){orders(first:$n){edges{node{id}}}}`} {
		b, e := Analyze(q, "", nil)
		if e != nil || b.Cost < 1 {
			t.Fatal(b, e)
		}
	}
	if _, e := Analyze(`query A{__typename}query B{__typename}`, "", nil); e == nil {
		t.Fatal("ambiguous operation accepted")
	}
	if _, e := Analyze(`query A{__typename}query B{__typename}`, "B", nil); e != nil {
		t.Fatal(e)
	}
	aliases := []string{}
	for i := 0; i < 9; i++ {
		aliases = append(aliases, fmt.Sprintf("a%d:product(sku:\"p\"){name}", i))
	}
	if _, e := Analyze("{"+strings.Join(aliases, " ")+"}", "", nil); e == nil {
		t.Fatal("alias root bound bypassed")
	}
	s := engine.MustParseSchema(Schema, nil)
	lists := map[string]bool{"OrderConnection.edges": true, "Order.lines": true, "ProductDetails.variants": true, "ProductDetails.categories": true}
	for name, typ := range s.AST().Types {
		obj, ok := typ.(*ast.ObjectTypeDefinition)
		if !ok || strings.HasPrefix(name, "__") {
			continue
		}
		for _, f := range obj.Fields {
			ft := f.Type
			if nn, ok := ft.(*ast.NonNull); ok {
				ft = nn.OfType
			}
			if list, ok := ft.(*ast.List); ok {
				if !lists[name+"."+f.Name] {
					t.Fatalf("list lacks cost bound: %s.%s", name, f.Name)
				}
				ft = list.OfType
				if nn, ok := ft.(*ast.NonNull); ok {
					ft = nn.OfType
				}
			}
			if target, ok := ft.(*ast.ObjectTypeDefinition); ok && childType(name, f.Name) != target.Name {
				t.Fatalf("cost traversal misses %s.%s", name, f.Name)
			}
		}
	}
}

func TestBatchFuturesNegativeCacheFailureAndLimit(t *testing.T) {
	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	b := newBatch(func(context.Context, []string) ([]string, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return []string{"same"}, nil
	}, func(s string) (string, bool) { return s, true })
	var wg sync.WaitGroup
	fail := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := b.load(context.Background(), []string{"same", "absent"})
			if e != nil {
				fail <- e
				return
			}
			if v["same"] == nil || v["absent"] != nil {
				fail <- fmt.Errorf("wrong cached values")
			}
		}()
	}
	<-started
	close(release)
	wg.Wait()
	close(fail)
	for e := range fail {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate future fetch", calls.Load())
	}
	if _, e := b.load(context.Background(), []string{"absent"}); e != nil || calls.Load() != 1 {
		t.Fatal("negative cache failed", e)
	}
	clean := newBatch(func(context.Context, []string) ([]string, error) { return nil, nil }, func(s string) (string, bool) { return s, true })
	keys := []string{}
	for i := 0; i < 2001; i++ {
		keys = append(keys, fmt.Sprint(i))
	}
	if _, e := clean.load(context.Background(), keys); e == nil || len(clean.items) != 0 {
		t.Fatal("limit left orphan futures")
	}
	if _, e := clean.load(context.Background(), []string{"valid"}); e != nil {
		t.Fatal(e)
	}
}

func TestLongAndCursorBoundaries(t *testing.T) {
	for _, s := range []string{"0", "-9223372036854775808", "9223372036854775807", "9007199254740993"} {
		var n Long
		if e := n.UnmarshalGraphQL(s); e != nil {
			t.Fatal(e)
		}
		wire, _ := json.Marshal(n)
		if string(wire) != strconvQuote(s) {
			t.Fatal(string(wire))
		}
	}
	for _, v := range []any{float64(1), "01", "-0", "9223372036854775808"} {
		var n Long
		if n.UnmarshalGraphQL(v) == nil {
			t.Fatal(v)
		}
	}
	c := cursorCodec{[]byte(strings.Repeat("k", 32))}
	v := c.encode("acme", fixtureID)
	if id, e := c.decode("acme", v); e != nil || id != fixtureID {
		t.Fatal(id, e)
	}
	if _, e := c.decode("globex", v); e == nil {
		t.Fatal("cursor tenant changed")
	}
	if _, e := (cursorCodec{[]byte(strings.Repeat("x", 32))}).decode("acme", v); e == nil {
		t.Fatal("cursor key changed")
	}
}
func strconvQuote(s string) string { b, _ := json.Marshal(s); return string(b) }

func FuzzQueryBudget(f *testing.F) {
	for _, s := range []string{`{orders(first:2){edges{node{lines{product{name}}}}}}`, `{...A}fragment A on Query{...A}`, `query($n:Int=2){orders(first:$n){edges{node{id}}}}`, `{a:product(sku:"p"){name}b:product(sku:"p"){priceMinor}}`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > MaxQueryBytes {
			t.Skip()
		}
		b, e := Analyze(s, "", nil)
		if e == nil && (b.Depth > MaxDepth || b.Cost > MaxCost || b.Selections > MaxSelections || b.Roots > MaxRoots) {
			t.Fatalf("unbounded accepted query %+v", b)
		}
	})
}
