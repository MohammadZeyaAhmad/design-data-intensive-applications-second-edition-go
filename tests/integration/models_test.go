//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	modelbench "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/benchmarks/models"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	cataloghttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/httpapi"
	catalogdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/describe"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/publish"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

func contentFixture(t *testing.T) (string, *catalogdb.Store, domain.Details) {
	t.Helper()
	id, e := identity.NewID()
	if e != nil {
		t.Fatal(e)
	}
	tenant := "content-" + id
	s := &catalogdb.Store{Pool: pool}
	ctx := context.Background()
	if _, _, e = (publish.Handler{Store: s}).Handle(ctx, tenant, "price", domain.Product{SKU: "device", Name: "Device", UnitPriceMinor: 1000, Currency: "USD"}); e != nil {
		t.Fatal(e)
	}
	if _, _, e = (describe.Handler{Store: s}).Brand(ctx, tenant, "brand", domain.Brand{ID: "maker", Name: "Maker"}); e != nil {
		t.Fatal(e)
	}
	return tenant, s, domain.Details{SKU: "device", BrandID: "maker", Description: "Details", Attributes: json.RawMessage(`{"weight_grams":120,"material":"recycled"}`), Variants: []domain.Variant{{Code: "blue", Label: "Blue"}, {Code: "red", Label: "Red"}}, Categories: []string{"electronics", "devices"}}
}
func TestCatalogModelsDetailsRevisionsReferencesAndHistory(t *testing.T) {
	tenant, s, d := contentFixture(t)
	ctx := context.Background()
	handler := describe.Handler{Store: s}
	out, replay, e := handler.Handle(ctx, tenant, "describe", d)
	if e != nil || replay || out.Version != 1 {
		t.Fatal(out, e)
	}
	again, replay, e := handler.Handle(ctx, tenant, "describe", d)
	if e != nil || !replay || again.Version != 1 {
		t.Fatal("request replay failed", e)
	}
	d.Description = "different"
	if _, _, e = handler.Handle(ctx, tenant, "describe", d); !errors.Is(e, commands.ErrConflict) {
		t.Fatal("changed request accepted")
	}
	if _, _, e = handler.Brand(ctx, tenant, "rename", domain.Brand{ID: "maker", Name: "Current maker", Version: 1}); e != nil {
		t.Fatal(e)
	}
	got, e := s.Details(ctx, tenant, "device")
	if e != nil || got.BrandName != "Current maker" || got.Version != 1 || len(got.Variants) != 2 || got.Categories[0] != "devices" {
		t.Fatal(got, e)
	}
	if _, e = s.Details(ctx, tenant+"-other", "device"); !errors.Is(e, api.ErrNotFound) {
		t.Fatal("tenant boundary failed", e)
	}
	d.BrandID = "missing"
	d.Version = 1
	if _, _, e = handler.Handle(ctx, tenant, "missing-reference", d); !errors.Is(e, api.ErrNotFound) {
		t.Fatal("missing reference accepted", e)
	}
	d.BrandID = "maker"
	d.Variants = d.Variants[:1]
	d.Categories = []string{}
	if _, _, e = handler.Handle(ctx, tenant, "replace", d); e != nil {
		t.Fatal(e)
	}
	got, e = s.Details(ctx, tenant, "device")
	if e != nil || got.Version != 2 || len(got.Variants) != 1 || len(got.Categories) != 0 {
		t.Fatal("child replacement not atomic", got, e)
	}
	var n int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM catalog.content_history WHERE tenant_id=$1 AND metadata->>'actor'='system:internal'`, tenant).Scan(&n); e != nil || n != 4 {
		t.Fatal("immutable history/provenance missing", n, e)
	}
	if _, e = pool.Exec(ctx, `UPDATE catalog.content_history SET body='{}' WHERE tenant_id=$1`, tenant); e == nil {
		t.Fatal("history mutable")
	}
	p, e := s.Product(ctx, tenant, "device")
	if e != nil || p.Version != 1 || p.UnitPriceMinor != 1000 {
		t.Fatal("content changed price authority", p, e)
	}
}
func TestCatalogModelsConcurrentFirstPublication(t *testing.T) {
	tenant, s, d := contentFixture(t)
	var wg sync.WaitGroup
	var won, conflicted atomic.Int64
	fail := make(chan error, 12)
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, e := (describe.Handler{Store: s}).Handle(context.Background(), tenant, fmt.Sprintf("first-%d", i), d)
			if e == nil {
				won.Add(1)
			} else if errors.Is(e, api.ErrConflict) {
				conflicted.Add(1)
			} else {
				fail <- e
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(fail)
	for e := range fail {
		t.Fatal(e)
	}
	if won.Load() != 1 || conflicted.Load() != 11 {
		t.Fatal("expected revision did not fence initial publication", won.Load(), conflicted.Load())
	}
}
func TestCatalogModelsHTTPContractsAndTenant(t *testing.T) {
	tenant, s, _ := contentFixture(t)
	token := "model-fixture-token"
	other := "model-other-token"
	server := httptest.NewServer(cataloghttp.New(s, map[[32]byte]string{sha256.Sum256([]byte(token)): tenant, sha256.Sum256([]byte(other)): tenant + "-other"}))
	defer server.Close()
	call := func(method, path, key, bearer string, body any, want int) []byte {
		t.Helper()
		var reader io.Reader
		if body != nil {
			b, e := json.Marshal(body)
			if e != nil {
				t.Fatal(e)
			}
			reader = bytes.NewReader(b)
		}
		req, e := http.NewRequest(method, server.URL+path, reader)
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		b, e := io.ReadAll(res.Body)
		if e != nil || res.StatusCode != want {
			t.Fatalf("%s %s: got %d want %d: %s", method, path, res.StatusCode, want, b)
		}
		return b
	}
	path := "/v1/catalog/products/device/details"
	body := map[string]any{"brand_id": "maker", "description": "Nested metadata", "attributes": map[string]any{"wireless": true}, "variants": []map[string]string{{"code": "blue", "label": "Blue"}}, "categories": []string{"devices"}, "expected_version": 0}
	call("GET", path, "", "", nil, 401)
	call("PUT", path, "detail-http", token, body, 200)
	call("PUT", path, "detail-http", token, body, 200)
	call("GET", path, "", other, nil, 404)
	call("PUT", "/v1/catalog/brands/maker", "rename-http", token, map[string]any{"name": "HTTP maker", "expected_version": 1}, 200)
	b := call("GET", path, "", token, nil, 200)
	var d domain.Details
	if json.Unmarshal(b, &d) != nil || d.BrandName != "HTTP maker" || d.Version != 1 {
		t.Fatal("joined current reference missing")
	}
	body["categories"] = []string{"duplicate", "duplicate"}
	call("PUT", path, "invalid", token, body, 400)
	body["categories"] = []string{}
	body["attributes"] = []string{"not-an-object"}
	call("PUT", path, "invalid-object", token, body, 400)
	var actor string
	if e := pool.QueryRow(context.Background(), `SELECT metadata->>'actor' FROM catalog.content_history WHERE tenant_id=$1 AND kind='details' AND id='device'`, tenant).Scan(&actor); e != nil || actor == "" || actor == "system:internal" {
		t.Fatal("HTTP actor provenance missing", e)
	}
}
func TestCatalogModelsRealRepresentationsAndORM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	rows, runs := 128, 1
	path := os.Getenv("ATLAS_MODELS_REPORT")
	if path != "" {
		rows, runs = 512, 3
	}
	r, e := modelbench.Run(ctx, pool, rows, runs)
	if e != nil {
		t.Fatal(e)
	}
	if !r.Passed || len(r.Mappings) != 3 || len(r.Plans) != 8 || len(r.Samples) != 16*runs {
		t.Fatal("incomplete modeling evidence")
	}
	if path != "" {
		if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			t.Fatal(e)
		}
		body, e := json.MarshalIndent(r, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, append(body, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
