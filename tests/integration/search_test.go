//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	searchhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/adapters/httpapi"
	searchdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	searchpublish "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/features/publish"
)

func searchDoc(t *testing.T, s *searchdb.Store, tenant, key string, d domain.Document) domain.Document {
	t.Helper()
	out, _, e := (searchpublish.Handler{Store: s}).Handle(context.Background(), tenant, key, d)
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func TestSearchTextSpatialUpdatesAndTenantIsolation(t *testing.T) {
	ctx := context.Background()
	a, b := tenant(t), tenant(t)
	s := &searchdb.Store{Pool: pool}
	d := searchDoc(t, s, a, "doc-a", domain.Document{ID: "mouse", SKU: "mouse-sku", Warehouse: "east", Title: "Wireless mouse", Body: "Quiet ergonomic computer peripheral", Point: &domain.Point{Lon: 179.8, Lat: 0}})
	searchDoc(t, s, a, "doc-b", domain.Document{ID: "animal", Title: "Field mouse", Body: "Small animal", Point: &domain.Point{Lon: -179.8, Lat: 0}})
	searchDoc(t, s, b, "private", domain.Document{ID: "secret", Title: "Wireless mouse", Body: "Private supplier", Point: &domain.Point{Lon: 179.9, Lat: 0}})
	hits, e := s.Text(ctx, a, `"wireless mouse"`, 10)
	if e != nil || len(hits) != 1 || hits[0].ID != "mouse" {
		t.Fatal(hits, e)
	}
	hits, e = s.Suggest(ctx, a, "wireles mouse", 10)
	if e != nil || len(hits) == 0 || hits[0].ID != "mouse" {
		t.Fatal("trigram correction", hits, e)
	}
	hits, e = s.Box(ctx, a, domain.Box{MinLon: 170, MaxLon: -170, MinLat: -5, MaxLat: 5}, 10)
	if e != nil || len(hits) != 2 {
		t.Fatal("wrapped range", hits, e)
	}
	hits, e = s.Radius(ctx, a, domain.Point{Lon: 180, Lat: 0}, 30, 10)
	if e != nil || len(hits) != 2 || hits[0].Distance > 23 {
		t.Fatal("spherical radius", hits, e)
	}
	d.Title = "Mechanical keyboard"
	d.Body = "Silent keys"
	d.Point = &domain.Point{Lon: 0, Lat: 30}
	d = searchDoc(t, s, a, "update", d)
	hits, e = s.Text(ctx, a, "wireless", 10)
	if e != nil || len(hits) != 0 {
		t.Fatal("stale FTS", hits, e)
	}
	hits, e = s.Radius(ctx, a, domain.Point{Lon: 180, Lat: 0}, 30, 10)
	if e != nil || len(hits) != 1 || hits[0].ID != "animal" {
		t.Fatal("stale point", hits, e)
	}
	if _, _, e = (searchpublish.Handler{Store: s}).Delete(ctx, a, "delete", d.ID, d.Version); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Document(ctx, a, d.ID); !errors.Is(e, domain.ErrNotFound) {
		t.Fatal(e)
	}
	if _, _, e = (searchpublish.Handler{Store: s}).Handle(ctx, a, "stale", d); !errors.Is(e, domain.ErrConflict) {
		t.Fatal("deleted document resurrected", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE search.document_history SET body='{}' WHERE tenant_id=$1`, a); e == nil {
		t.Fatal("history mutable")
	}
}
func TestSearchIdempotentConcurrentExpectedVersion(t *testing.T) {
	ctx := context.Background()
	id := tenant(t)
	s := &searchdb.Store{Pool: pool}
	d := domain.Document{ID: "item", Title: "Warehouse glove", Body: "Safety stock"}
	first, replayed, e := (searchpublish.Handler{Store: s}).Handle(ctx, id, "same", d)
	if e != nil || replayed {
		t.Fatal(e)
	}
	again, replayed, e := (searchpublish.Handler{Store: s}).Handle(ctx, id, "same", d)
	if e != nil || !replayed || !reflect.DeepEqual(first, again) {
		t.Fatal("replay", e)
	}
	d.Title = "different"
	if _, _, e = (searchpublish.Handler{Store: s}).Handle(ctx, id, "same", d); !errors.Is(e, commands.ErrConflict) {
		t.Fatal("payload conflict", e)
	}
	d = first
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			next := d
			next.Title = fmt.Sprintf("glove %d", i)
			_, _, e := (searchpublish.Handler{Store: s}).Handle(ctx, id, fmt.Sprintf("race-%d", i), next)
			results <- e
		}(i)
	}
	wg.Wait()
	close(results)
	ok, conflicts := 0, 0
	for e := range results {
		if e == nil {
			ok++
		} else if errors.Is(e, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(e)
		}
	}
	if ok != 1 || conflicts != 1 {
		t.Fatal("version fence", ok, conflicts)
	}
}
func TestSearchHTTPAuthenticationMetadataAndVectorBoundary(t *testing.T) {
	a, b := tenant(t), tenant(t)
	keys := map[[32]byte]string{sha256.Sum256([]byte("search-key-a")): a, sha256.Sum256([]byte("search-key-b")): b}
	handler := searchhttp.New(&searchdb.Store{Pool: pool}, keys)
	call := func(method, path, key string, body any, token string) *httptest.ResponseRecorder {
		var data []byte
		if body != nil {
			data, _ = json.Marshal(body)
		}
		r := httptest.NewRequest(method, path, bytes.NewReader(data))
		if body != nil {
			r.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	body := map[string]any{"title": "Discovery mouse", "body": "Wireless stock", "sku": "mouse", "expected_version": 0}
	if w := call(http.MethodPut, "/v1/search/documents/item", "first", body, "search-key-a"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(http.MethodPut, "/v1/search/documents/item", "first", body, "search-key-a"); w.Code != 200 || w.Header().Get("Idempotent-Replay") != "true" {
		t.Fatal("HTTP replay", w.Code)
	}
	if w := call(http.MethodGet, "/v1/search/text?q=mouse", "", nil, ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	w := call(http.MethodGet, "/v1/search/text?q=mouse", "", nil, "search-key-b")
	var hits []api.Hit
	if e := json.Unmarshal(w.Body.Bytes(), &hits); e != nil || w.Code != 200 || len(hits) != 0 {
		t.Fatal("tenant escaped", w.Code, w.Body.String())
	}
	if w := call(http.MethodGet, "/v1/search/near?lon=NaN&lat=0&km=1", "", nil, "search-key-a"); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(http.MethodPost, "/v1/search/vector", "", map[string]any{"model": "synthetic", "model_version": "1", "values": []float64{1, 0, 0, 0, 0, 0, 0, 0}, "limit": 10}, "search-key-a"); w.Code != 503 {
		t.Fatal("disabled vector silently changed engine", w.Code, w.Body.String())
	}
	var actor string
	if e := pool.QueryRow(context.Background(), `SELECT metadata->>'actor' FROM search.document_history WHERE tenant_id=$1 AND id='item'`, a).Scan(&actor); e != nil || actor != "tenant-key:"+a {
		t.Fatal("server provenance", actor, e)
	}
}
