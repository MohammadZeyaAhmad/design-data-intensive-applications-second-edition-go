package models

import (
	"encoding/json"
	"errors"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func oracle(f Fixture, q Query) []Card {
	out := []Card{}
	allowed := map[string]bool{}
	for _, c := range f.Customers {
		if c.Tenant == q.Tenant && c.ID == q.Customer {
			for _, sku := range c.Favorites {
				allowed[sku] = true
			}
		}
	}
	for _, p := range f.Products {
		if p.Tenant != q.Tenant || p.Price > q.MaxPrice || q.Customer != "" && !allowed[p.SKU] {
			continue
		}
		match := false
		for _, c := range p.Categories {
			if c == q.Category {
				match = true
			}
		}
		if !match {
			continue
		}
		brand := ""
		for _, b := range f.Brands {
			if b.Tenant == p.Tenant && b.ID == p.BrandID {
				brand = b.Name
			}
		}
		out = append(out, Card{p.SKU, p.Name, brand, p.Price, len(p.Variants)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Price != out[j].Price {
			return out[i].Price < out[j].Price
		}
		return out[i].SKU < out[j].SKU
	})
	if len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out
}
func TestRepresentationsAgainstIndependentQuestions(t *testing.T) {
	f := FixtureFor(384)
	path := filepath.Join(t.TempDir(), "models.bolt")
	s, e := Open(path, &f)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { s.Close() }()
	rng := rand.New(rand.NewSource(301))
	// Duplicate SKU across tenants and equal prices expose partition/tie errors.
	f.Products[1].SKU = f.Products[0].SKU
	f.Products[1].Price = f.Products[0].Price
	for i := range f.Customers {
		if f.Customers[i].Tenant == "globex" {
			for j, sku := range f.Customers[i].Favorites {
				if sku == "sku-00001" {
					f.Customers[i].Favorites[j] = f.Products[1].SKU
				}
			}
		}
	}
	s.Close()
	s, e = Open(filepath.Join(t.TempDir(), "ties.bolt"), &f)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 160; i++ {
		tenant := []string{"acme", "globex", "missing"}[rng.Intn(3)]
		category := []string{"all", "category-0", "category-1", "missing"}[rng.Intn(4)]
		customer := []string{"", "customer-0", "customer-1", "missing"}[rng.Intn(4)]
		q := Query{tenant, category, customer, int64(rng.Intn(10000)), 1 + rng.Intn(100)}
		want := oracle(f, q)
		for _, mode := range []string{"normalized", "document", "denormalized", "wide-column"} {
			got, e := s.Query(mode, q)
			if e != nil || !reflect.DeepEqual(got.Cards, want) {
				t.Fatalf("%s query %+v differs: %v", mode, q, e)
			}
		}
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	empty, e := Open(filepath.Join(filepath.Dir(path), "absent.bolt"), nil)
	if e == nil {
		empty.Close()
		t.Fatal("adopted empty file without fixture")
	}
}
func TestAnomalyRepairDeleteAndReopen(t *testing.T) {
	r, e := RunScenarioAt(t.TempDir(), 64, 1)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Samples) != 16 || r.NestedDocumentBytes <= r.VariantRowBytes {
		t.Fatal("incomplete scenario")
	}
}
func TestConcurrentQueriesAndRepair(t *testing.T) {
	f := FixtureFor(64)
	s, e := Open(filepath.Join(t.TempDir(), "models.bolt"), &f)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var wg sync.WaitGroup
	fail := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 12; j++ {
				if i == 0 {
					if e := s.Repair(); e != nil {
						fail <- e
						return
					}
				} else {
					_, e := s.Query([]string{"normalized", "document", "wide-column"}[i%3], Query{"acme", "all", "", 1e12, 100})
					if e != nil {
						fail <- e
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
func TestSchemaEvolutionAndFixtureBounds(t *testing.T) {
	for _, body := range []string{`{"schema":1,"weight_grams":"0"}`, `{"schema":2,"weight_grams":0}`} {
		v, _, e := DecodeWeight([]byte(body))
		if e != nil || v == nil || *v != 0 {
			t.Fatal(body, e)
		}
	}
	for _, body := range []string{`{"schema":2}`, `{"schema":1,"weight_grams":null}`} {
		v, _, e := DecodeWeight([]byte(body))
		if e != nil || v != nil {
			t.Fatal(body, e)
		}
	}
	for _, body := range []string{`{"schema":3,"weight_grams":1}`, `{"schema":2,"weight_grams":"1"}`, `{"schema":1,"weight_grams":1}`, `{"schema":2,"weight_grams":-1}`, `{"schema":2,"weight_grams":1.5}`, `{"schema":2,"weight_grams":1,"other":2}`, `{"schema":2} {}`} {
		if _, _, e := DecodeWeight([]byte(body)); !errors.Is(e, ErrInvalid) {
			t.Fatal("accepted drift", body)
		}
	}
	f := FixtureFor(32)
	f.Products[0].BrandID = "missing"
	if e := validate(f); !errors.Is(e, ErrInvalid) {
		t.Fatal("missing reference accepted")
	}
	f = FixtureFor(32)
	f.Products[0].Categories = append(f.Products[0].Categories, "all")
	if e := validate(f); e == nil {
		t.Fatal("duplicate relationship accepted")
	}
}
func FuzzSchemaOnRead(f *testing.F) {
	for _, body := range []string{`{"schema":1,"weight_grams":"120"}`, `{"schema":2,"weight_grams":0}`, `{"schema":2,"weight_grams":null}`, `{}`} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		v, _, e := DecodeWeight(body)
		if e == nil && v != nil {
			if *v < 0 || *v > 1e9 {
				t.Fatal("invalid migrated value")
			}
			canonical, _ := json.Marshal(map[string]any{"schema": 2, "weight_grams": *v})
			r, legacy, e := DecodeWeight(canonical)
			if e != nil || legacy || r == nil || *r != *v {
				t.Fatal("migration failed round trip")
			}
		}
	})
}
