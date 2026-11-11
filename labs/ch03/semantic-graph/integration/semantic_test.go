//go:build semanticintegration

package integration

import (
	"context"
	"errors"
	"fmt"
	graph "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/property-graph"
	semantic "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/native"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/rdf"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/sparql"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func contextFor(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}
func rdfClient(t *testing.T) *sparql.Client {
	t.Helper()
	c, e := sparql.NewClient(os.Getenv("ATLAS_RDF_URL"), os.Getenv("ATLAS_RDF_PASSWORD"))
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func storeFor(t *testing.T, ctx context.Context, c *sparql.Client) *sparql.Store {
	t.Helper()
	s, e := sparql.New(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if e := s.Close(cleanup); e != nil {
			t.Error(e)
		}
	})
	return s
}

func TestFiveBackendsAcrossRebuilds(t *testing.T) {
	r, e := native.Run(contextFor(t), 32, 1)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Samples) != 210 || len(r.Plans) != 4 || len(r.Publications) != 15 {
		t.Fatalf("incomplete native report: samples=%d plans=%d", len(r.Samples), len(r.Plans))
	}
}

func TestRealSPARQLTermsBagsAndInteroperability(t *testing.T) {
	ctx := contextFor(t)
	c := rdfClient(t)
	s := storeFor(t, ctx, c)
	f, _ := graph.Synthetic(32)
	if e := s.Replace(ctx, f); e != nil {
		t.Fatal(e)
	}
	source, _ := s.Source("acme")
	query := fmt.Sprintf(`PREFIX a: <%s> SELECT ?to WHERE { GRAPH %s { ?edge a:from <urn:atlas:tenant:acme:node:wh-000>; a:to ?to . FILTER(?to=<urn:atlas:tenant:acme:node:wh-001>) } }`, rdf.Vocab, source.String())
	bag, e := c.Select(ctx, query)
	if e != nil {
		t.Fatal(e)
	}
	distinct, e := c.Select(ctx, strings.Replace(query, "SELECT ?to", "SELECT DISTINCT ?to", 1))
	if e != nil {
		t.Fatal(e)
	}
	if len(bag) != 2 || len(distinct) != 1 {
		t.Fatalf("bag=%d distinct=%d", len(bag), len(distinct))
	}
	ok, e := c.Ask(ctx, `PREFIX x: <http://www.w3.org/2001/XMLSchema#> ASK { FILTER(!sameTerm("01"^^x:integer,"1"^^x:integer) && "01"^^x:integer = "1"^^x:integer) }`)
	if e != nil || !ok {
		t.Fatalf("lexical identity vs numeric equality: %v %v", ok, e)
	}
	id, e := native.ID()
	if e != nil {
		t.Fatal(e)
	}
	uri := rdf.URI("urn:atlas:interop:" + id)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := c.Update(cleanup, "DROP SILENT GRAPH "+uri.String()); e != nil {
			t.Error(e)
		}
	})
	turtle := `@prefix other: <https://atlas.example/vocab#> .
 @prefix x: <http://www.w3.org/2001/XMLSchema#> .
 <urn:atlas:external:warehouse> a other:Warehouse; other:id "external"; other:quantity "01"^^x:integer .
 _:local other:label "bonjour"@FR .`
	for i := 0; i < 2; i++ {
		if e = c.LoadTurtle(ctx, uri, turtle); e != nil {
			t.Fatal(e)
		}
	}
	ok, e = c.Ask(ctx, fmt.Sprintf(`PREFIX a: <%s> ASK { GRAPH %s { <urn:atlas:external:warehouse> a a:Warehouse; a:id "external" . ?b a:label "bonjour"@fr } }`, rdf.Vocab, uri.String()))
	if e != nil || !ok {
		t.Fatalf("Turtle/prefix/language interoperability %v %v", ok, e)
	}
	rows, e := c.Select(ctx, fmt.Sprintf(`PREFIX a: <%s> SELECT (COUNT(DISTINCT ?b) AS ?count) WHERE {GRAPH %s {?b a:label ?label}}`, rdf.Vocab, uri.String()))
	if e != nil || len(rows) != 1 || rows[0]["count"].Value != "2" {
		t.Fatalf("document blank scope %v %v", rows, e)
	}
}

func TestNativeTenantFencesAuthCancellationAndOwnedCleanup(t *testing.T) {
	ctx := contextFor(t)
	c := rdfClient(t)
	s := storeFor(t, ctx, c)
	f, _ := graph.Synthetic(32)
	if e := s.Replace(ctx, f); e != nil {
		t.Fatal(e)
	}
	source, _ := s.Source("acme")
	// An incorrectly placed foreign node and edges must not grant reachability.
	update := fmt.Sprintf(`PREFIX a: <%s> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
 INSERT DATA {GRAPH %s {
 <urn:atlas:tenant:globex:node:foreign> a a:Warehouse; a:owner <urn:atlas:tenant:globex>; a:id "foreign"; rdfs:label "foreign" .
 <urn:atlas:tenant:acme:edge:bridge> a a:Route; a:owner <urn:atlas:tenant:acme>; a:from <urn:atlas:tenant:acme:node:wh-000>; a:to <urn:atlas:tenant:globex:node:foreign>; a:enabled true; a:capacity 100 .
 <urn:atlas:tenant:acme:edge:wrong-owner> a a:Route; a:owner <urn:atlas:tenant:globex>; a:from <urn:atlas:tenant:acme:node:wh-000>; a:to <urn:atlas:tenant:acme:node:wh-012>; a:enabled true; a:capacity 100 .
 }}`, rdf.Vocab, source.String())
	if e := c.Update(ctx, update); e != nil {
		t.Fatal(e)
	}
	q := semantic.Cases(32)[0].Query
	got, e := s.Query(ctx, q)
	want, _ := semantic.Oracle(ctx, f, q)
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("tenant bridge leaked: %v %v", got, e)
	}
	q.Start = "missing"
	got, e = s.Query(ctx, q)
	if e != nil || len(got) != 0 {
		t.Fatal("phantom zero-hop start", got, e)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = s.Query(canceled, q); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.Base()+"/query", nil)
	if e != nil {
		t.Fatal(e)
	}
	resp, e := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status %d", resp.StatusCode)
	}
	// Concurrent scratch graphs must be independent, and fixture publication
	// must never expose a partially replaced source to this store's readers.
	q = semantic.Cases(32)[0].Query
	updated := graph.Updated(f)
	wantOld, _ := semantic.Oracle(ctx, f, q)
	wantNew, _ := semantic.Oracle(ctx, updated, q)
	var wg sync.WaitGroup
	problems := make(chan error, 3)
	for worker := 0; worker < 2; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				a, e := s.Query(ctx, q)
				if e != nil {
					problems <- e
					return
				}
				if !reflect.DeepEqual(a, wantOld) && !reflect.DeepEqual(a, wantNew) {
					problems <- semantic.ErrInvalid
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, fixture := range []graph.Fixture{updated, f} {
			if e := s.Replace(ctx, fixture); e != nil {
				problems <- e
				return
			}
		}
	}()
	wg.Wait()
	close(problems)
	for e := range problems {
		t.Fatal(e)
	}
	other := storeFor(t, ctx, c)
	if e = other.Replace(ctx, f); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(ctx); e != nil {
		t.Fatal(e)
	}
	exists, e := c.Ask(ctx, "ASK {GRAPH "+source.String()+" {?s ?p ?o}}")
	if e != nil || exists {
		t.Fatal("owned source survived cleanup", e)
	}
	prefix := strings.TrimSuffix(source.Value, "source:acme")
	exists, e = c.Ask(ctx, fmt.Sprintf(`ASK {GRAPH ?g {?s ?p ?o} FILTER(STRSTARTS(STR(?g),%q))}`, prefix))
	if e != nil || exists {
		t.Fatal("owned scratch/source graph survived cleanup", e)
	}
	otherSource, _ := other.Source("acme")
	exists, e = c.Ask(ctx, "ASK {GRAPH "+otherSource.String()+" {?s ?p ?o}}")
	if e != nil || !exists {
		t.Fatal("unrelated source deleted", e)
	}
}
