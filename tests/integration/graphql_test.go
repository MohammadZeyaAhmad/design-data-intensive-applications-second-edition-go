//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	graphapi "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/graphql"
	experiment "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/benchmarks/graphql"
	cataloghttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/httpapi"
	catalogdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	catalogdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/describe"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/publish"
	quotehandler "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/quote"
	orderhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/httpapi"
	orderdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	orderdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/placeorder"
	"github.com/jackc/pgx/v5/pgxpool"
)

const graphqlToken = "graphql-integration-disposable-only"

type graphqlFixture struct {
	tenant, id string
	catalog    *catalogdb.Store
	orders     *orderdb.Store
	router     http.Handler
}

func newGraphQLFixture(t *testing.T, n int) *graphqlFixture {
	t.Helper()
	ctx := context.Background()
	// Reuse the harness's tenant teardown: the frozen-price scenario accepts
	// checkout without running it, so its delivery lane must not leak into a
	// later test's global dispatcher.
	tenant := commerceTenant(t)
	var e error
	cat := &catalogdb.Store{Pool: pool}
	orders := &orderdb.Store{Pool: pool}
	if _, _, e = (describe.Handler{Store: cat}).Brand(ctx, tenant, "brand", catalogdomain.Brand{ID: "maker", Name: "Maker"}); e != nil {
		t.Fatal(e)
	}
	lines := []orderdomain.Line{}
	for i := 0; i < n; i++ {
		sku := fmt.Sprintf("p-%03d", i)
		if _, _, e = (publish.Handler{Store: cat}).Handle(ctx, tenant, "price-"+sku, catalogdomain.Product{SKU: sku, Name: "Product " + sku, UnitPriceMinor: int64(1000 + i), Currency: "USD"}); e != nil {
			t.Fatal(e)
		}
		if _, _, e = (describe.Handler{Store: cat}).Handle(ctx, tenant, "detail-"+sku, catalogdomain.Details{SKU: sku, BrandID: "maker", Description: "Details " + sku, Attributes: json.RawMessage(`{}`), Categories: []string{"devices"}, Variants: []catalogdomain.Variant{{Code: "blue", Label: "Blue"}}}); e != nil {
			t.Fatal(e)
		}
		lines = append(lines, orderdomain.Line{SKU: sku, Quantity: int64(i + 1)})
	}
	state, _, e := (placeorder.Handler{Store: orders}).Handle(ctx, tenant, "place", placeorder.Command{Lines: lines})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = orders.Rebuild(ctx, tenant, state.ID); e != nil {
		t.Fatal(e)
	}
	cfg := pool.Config().Copy()
	cfg.ConnConfig.Tracer = experiment.QueryTracer{}
	reads, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(reads.Close)
	keys := map[[32]byte]string{sha256.Sum256([]byte(graphqlToken)): tenant, sha256.Sum256([]byte(graphqlToken + "-other")): tenant + "-other"}
	readOrders := &orderdb.Store{Pool: reads}
	readCatalog := &catalogdb.Store{Pool: reads}
	graph, e := graphapi.New(graphapi.Options{Orders: readOrders, Catalog: readCatalog, TenantKeys: keys, CursorKey: []byte(strings.Repeat("g", 32))})
	if e != nil {
		t.Fatal(e)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/graphql", graph)
	mux.Handle("/v1/orders/", orderhttp.New(orderhttp.Options{Store: readOrders, TenantKeys: keys}))
	mux.Handle("/v1/catalog/", cataloghttp.New(readCatalog, keys))
	return &graphqlFixture{tenant, state.ID, cat, orders, mux}
}
func graphRequest(t *testing.T, f *graphqlFixture, token, query string, vars map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
	r := httptest.NewRequest("POST", "/v1/graphql", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}

func TestGraphQLNativeBatchCountsAndCompleteRESTParity(t *testing.T) {
	query, e := experiment.CompositionQuery("../..")
	if e != nil {
		t.Fatal(e)
	}
	report := experiment.Report{Mode: "actual PostgreSQL query executions through in-process GraphQL and REST HTTP handlers", Definitions: experiment.Definitions()}
	for _, n := range []int{1, 10, 100} {
		f := newGraphQLFixture(t, n)
		sample, e := experiment.Compare(context.Background(), f.router, graphqlToken, f.id, query, n)
		if e != nil {
			t.Fatal(n, e)
		}
		report.Samples = append(report.Samples, sample)
	}
	report.Passed = true
	if path := os.Getenv("ATLAS_GRAPHQL_REPORT"); path != "" {
		if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			t.Fatal(e)
		}
		b, _ := json.MarshalIndent(report, "", "  ")
		if e = os.WriteFile(path, append(b, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}

func TestGraphQLNativeTenantPaginationAndProjectionCorruption(t *testing.T) {
	f := newGraphQLFixture(t, 3)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		state, _, e := (placeorder.Handler{Store: f.orders}).Handle(ctx, f.tenant, fmt.Sprintf("page-%d", i), placeorder.Command{Lines: []orderdomain.Line{{SKU: "p-000", Quantity: 1}}})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.orders.Rebuild(ctx, f.tenant, state.ID); e != nil {
			t.Fatal(e)
		}
	}
	query := `query($after:String){orders(first:2,after:$after){edges{node{id}}pageInfo{endCursor hasNextPage}}}`
	ids := map[string]bool{}
	cursor := ""
	for page := 0; page < 2; page++ {
		vars := map[string]any{}
		if cursor != "" {
			vars["after"] = cursor
		}
		w := graphRequest(t, f, graphqlToken, query, vars)
		var r struct {
			Data struct {
				Orders struct {
					Edges    []struct{ Node struct{ ID string } }
					PageInfo struct {
						EndCursor   string
						HasNextPage bool
					}
				}
			}
			Errors []any
		}
		if json.Unmarshal(w.Body.Bytes(), &r) != nil || w.Code != 200 || len(r.Errors) > 0 {
			t.Fatal(w.Body)
		}
		for _, edge := range r.Data.Orders.Edges {
			if ids[edge.Node.ID] {
				t.Fatal("duplicate keyset row")
			}
			ids[edge.Node.ID] = true
		}
		cursor = r.Data.Orders.PageInfo.EndCursor
		if r.Data.Orders.PageInfo.HasNextPage != (page == 0) {
			t.Fatal(w.Body)
		}
	}
	if len(ids) != 4 {
		t.Fatal(ids)
	}
	w := graphRequest(t, f, graphqlToken+"-other", `{order(id:"`+f.id+`"){id lines{product{name}}}product(sku:"p-000"){name}}`, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"order":null`) || !strings.Contains(w.Body.String(), `"product":null`) {
		t.Fatal(w.Body)
	}
	w = graphRequest(t, f, graphqlToken+"-other", query, map[string]any{"after": cursor})
	if !strings.Contains(w.Body.String(), "BAD_USER_INPUT") {
		t.Fatal(w.Body)
	}
	if _, e := pool.Exec(ctx, `UPDATE ordering.order_views SET body=jsonb_set(body,'{tenant_id}','"foreign-secret"') WHERE tenant_id=$1 AND order_id=$2::uuid`, f.tenant, f.id); e != nil {
		t.Fatal(e)
	}
	w = graphRequest(t, f, graphqlToken, `{order(id:"`+f.id+`"){id}}`, nil)
	if strings.Contains(w.Body.String(), "foreign-secret") || !strings.Contains(w.Body.String(), "UNAVAILABLE") {
		t.Fatal(w.Body)
	}
}

func TestGraphQLNativeCurrentCatalogAndFrozenOrderPrices(t *testing.T) {
	f := newGraphQLFixture(t, 1)
	ctx := context.Background()
	q, _, e := (quotehandler.Handler{Store: f.catalog}).Handle(ctx, f.tenant, "quote", quotehandler.Command{Lines: []catalogdomain.Line{{SKU: "p-000", Quantity: 1}}})
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = (orderdb.Checkout{Store: f.orders}).Begin(ctx, f.tenant, f.id, "checkout", 1, q, "test-ok"); e != nil {
		t.Fatal(e)
	}
	query := `{order(id:"` + f.id + `",minVersion:"2"){version lines{sku frozenUnitPriceMinor frozenPriceVersion product{unitPriceMinor version}}}}`
	w := graphRequest(t, f, graphqlToken, query, nil)
	if !strings.Contains(w.Body.String(), "STALE_PROJECTION") {
		t.Fatal(w.Body)
	}
	if _, _, e = (publish.Handler{Store: f.catalog}).Handle(ctx, f.tenant, "reprice", catalogdomain.Product{SKU: "p-000", Name: "New product", UnitPriceMinor: 2000, Currency: "USD", Version: 1}); e != nil {
		t.Fatal(e)
	}
	if _, e = f.orders.Rebuild(ctx, f.tenant, f.id); e != nil {
		t.Fatal(e)
	}
	w = graphRequest(t, f, graphqlToken, query, nil)
	if w.Code != 200 || strings.Contains(w.Body.String(), `"errors"`) || !strings.Contains(w.Body.String(), `"frozenUnitPriceMinor":"1000"`) || !strings.Contains(w.Body.String(), `"unitPriceMinor":"2000"`) {
		t.Fatal(w.Body)
	}
}

func TestGraphQLNativeBudgetRejectsBeforeSQL(t *testing.T) {
	f := newGraphQLFixture(t, 1)
	ctx, counter := experiment.WithCounter(context.Background())
	query := `query($n:Int!){orders(first:$n){edges{node{lines{product{details{variants{code label}}}}}}}}`
	body, _ := json.Marshal(map[string]any{"query": query, "variables": map[string]any{"n": 20}})
	r := httptest.NewRequest("POST", "/v1/graphql", bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+graphqlToken)
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	if w.Code != 400 || counter.Load() != 0 || !strings.Contains(w.Body.String(), "QUERY_LIMIT") {
		t.Fatal(counter.Load(), w.Body)
	}
}
