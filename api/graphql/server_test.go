package graphql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
)

const fixtureToken = "graphql-test-token-disposable-only"
const fixtureID = "00000000-0000-0000-0000-000000000001"

type fixture struct {
	mu                       sync.Mutex
	calls                    [3]int
	orders                   []orders.Projection
	products                 map[string]api.ProductView
	details                  map[string]api.DetailView
	productOwner, brandOwner string
	failure                  bool
}

func newFixture(n int) *fixture {
	f := &fixture{products: map[string]api.ProductView{}, details: map[string]api.DetailView{}}
	for j := 1; j <= 4; j++ {
		o := orders.Projection{TenantID: "acme", ID: fmt.Sprintf("00000000-0000-0000-0000-%012d", j), Version: 1, Status: "placed", Lines: []orders.ProjectionLine{}}
		for i := 0; i < n; i++ {
			sku := fmt.Sprintf("p-%03d", i)
			o.Lines = append(o.Lines, orders.ProjectionLine{SKU: sku, Quantity: int64(i + 1)})
			f.products[sku] = api.ProductView{TenantID: "acme", SKU: sku, Name: "Product " + sku, UnitPriceMinor: 1000, Currency: "USD", Version: 1}
			f.details[sku] = api.DetailView{TenantID: "acme", SKU: sku, Description: "Details", Version: 1, Brand: api.BrandView{TenantID: "acme", ID: "maker", Name: "Maker"}, Categories: []string{"devices"}, Variants: []api.VariantView{{Code: "blue", Label: "Blue"}}}
		}
		f.orders = append(f.orders, o)
	}
	return f
}
func (f *fixture) ReadOrderProjections(ctx context.Context, tenant string, ids []string) ([]orders.Projection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[0]++
	out := []orders.Projection{}
	for _, o := range f.orders {
		for _, id := range ids {
			if o.TenantID == tenant && o.ID == id {
				out = append(out, o)
			}
		}
	}
	return out, ctx.Err()
}
func (f *fixture) ListOrderProjections(ctx context.Context, tenant, after string, first int) (orders.ProjectionPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[0]++
	out := orders.ProjectionPage{Nodes: []orders.Projection{}}
	for _, o := range f.orders {
		if o.TenantID == tenant && o.ID > after {
			out.Nodes = append(out.Nodes, o)
		}
	}
	if len(out.Nodes) > first {
		out.HasNextPage = true
		out.Nodes = out.Nodes[:first]
	}
	return out, ctx.Err()
}
func (f *fixture) ReadProducts(ctx context.Context, tenant string, skus []string) ([]api.ProductView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[1]++
	if f.failure {
		return nil, fmt.Errorf("private database credential must never reach the client")
	}
	out := []api.ProductView{}
	for _, sku := range skus {
		p, ok := f.products[sku]
		if ok && p.TenantID == tenant {
			if f.productOwner != "" {
				p.TenantID = f.productOwner
				p.Name = "foreign-secret"
			}
			out = append(out, p)
		}
	}
	return out, ctx.Err()
}
func (f *fixture) ReadProductDetails(ctx context.Context, tenant string, skus []string) ([]api.DetailView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[2]++
	out := []api.DetailView{}
	for _, sku := range skus {
		d, ok := f.details[sku]
		if ok && d.TenantID == tenant {
			if f.brandOwner != "" {
				d.Brand.TenantID = f.brandOwner
				d.Brand.Name = "foreign-secret"
			}
			out = append(out, d)
		}
	}
	return out, ctx.Err()
}
func testHandler(t *testing.T, f *fixture, policy func(context.Context, FieldScope) bool) http.Handler {
	t.Helper()
	h, e := New(Options{Orders: f, Catalog: f, TenantKeys: map[[32]byte]string{sha256.Sum256([]byte(fixtureToken)): "acme"}, CursorKey: []byte(strings.Repeat("k", 32)), AuthorizeField: policy})
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func execute(t *testing.T, h http.Handler, query string, vars map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
	r := httptest.NewRequest("POST", "/v1/graphql", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer "+fixtureToken)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func successful(t *testing.T, w *httptest.ResponseRecorder) map[string]json.RawMessage {
	t.Helper()
	var r struct {
		Data   map[string]json.RawMessage
		Errors []any
	}
	if e := json.Unmarshal(w.Body.Bytes(), &r); e != nil || w.Code != 200 || len(r.Errors) > 0 {
		t.Fatalf("HTTP %d: %s", w.Code, w.Body)
	}
	return r.Data
}

func TestBatchesOneTenAndHundredLines(t *testing.T) {
	for _, n := range []int{1, 10, 100} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := newFixture(n)
			h := testHandler(t, f, nil)
			q := `query($id:ID!){order(id:$id){id version lines{sku quantity p:product{sku name unitPriceMinor details{revision description brand{id name} categories variants{code label}}} again:product{sku}}}}`
			r := successful(t, execute(t, h, q, map[string]any{"id": fixtureID}))
			var order struct{ Lines []any }
			json.Unmarshal(r["order"], &order)
			if len(order.Lines) != n || f.calls != [3]int{1, 1, 1} {
				t.Fatalf("lines %d calls %v", len(order.Lines), f.calls)
			}
		})
	}
	f := newFixture(20)
	h := testHandler(t, f, nil)
	successful(t, execute(t, h, `{orders(first:2){edges{node{lines{product{name details{brand{name}}}}}}pageInfo{hasNextPage endCursor}}}`, nil))
	if f.calls != [3]int{1, 1, 1} {
		t.Fatal("page was not batched", f.calls)
	}
}

func TestSelectionFragmentsMissingAndRequestCache(t *testing.T) {
	conditional := newFixture(1)
	successful(t, execute(t, testHandler(t, conditional, nil), `query($show:Boolean!){order(id:"`+fixtureID+`"){lines{sku product @include(if:$show){name}}}}`, map[string]any{"show": false}))
	if conditional.calls != [3]int{1, 0, 0} {
		t.Fatal("skipped field was prefetched", conditional.calls)
	}
	f := newFixture(10)
	delete(f.products, "p-003")
	h := testHandler(t, f, nil)
	q := `query($show:Boolean!){order(id:"` + fixtureID + `"){lines{...L}}} fragment L on OrderLine{sku product @include(if:$show){name}}`
	successful(t, execute(t, h, q, map[string]any{"show": true}))
	if f.calls != [3]int{1, 1, 0} {
		t.Fatal(f.calls)
	}
	f.mu.Lock()
	p := f.products["p-000"]
	p.Name = "New name"
	f.products[p.SKU] = p
	f.mu.Unlock()
	r := execute(t, h, `{product(sku:"p-000"){name}}`, nil)
	successful(t, r)
	if !strings.Contains(r.Body.String(), "New name") {
		t.Fatal("cache leaked across requests")
	}
	before := f.calls
	successful(t, execute(t, h, `{order(id:"`+fixtureID+`"){lines{sku quantity}}}`, nil))
	if f.calls[1] != before[1] || f.calls[2] != before[2] {
		t.Fatal("unselected Catalog read")
	}
	successful(t, execute(t, h, `{product(sku:"foreign-only"){name}}`, nil))
}

func TestTenantOwnershipAndFieldPolicy(t *testing.T) {
	pageFixture := newFixture(1)
	pageHandler := testHandler(t, pageFixture, func(_ context.Context, s FieldScope) bool {
		return s.Permission != "projection.read" || s.ID != fixtureID
	})
	page := execute(t, pageHandler, `{orders(first:1){edges{node{id lines{product{name}}}}}}`, nil)
	if !strings.Contains(page.Body.String(), "FORBIDDEN") || pageFixture.calls[1] != 0 {
		t.Fatal("page bypassed per-order authorization", page.Body)
	}
	for _, kind := range []string{"product", "brand"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(1)
			if kind == "product" {
				f.productOwner = "globex"
			} else {
				f.brandOwner = "globex"
			}
			w := execute(t, testHandler(t, f, nil), `{product(sku:"p-000"){name details{brand{name}}}}`, nil)
			if strings.Contains(w.Body.String(), "foreign-secret") || !strings.Contains(w.Body.String(), "FORBIDDEN") {
				t.Fatal(w.Body)
			}
		})
	}
	f := newFixture(1)
	h := testHandler(t, f, func(_ context.Context, s FieldScope) bool { return s.Permission != "catalog.price.read" })
	w := execute(t, h, `{product(sku:"p-000"){name unitPriceMinor old:priceMinor}}`, nil)
	if w.Code != 200 || strings.Count(w.Body.String(), "FORBIDDEN") != 2 || strings.Contains(w.Body.String(), `"1000"`) {
		t.Fatal(w.Body)
	}
	f = newFixture(1)
	h = testHandler(t, f, func(_ context.Context, s FieldScope) bool { return s.Permission != "catalog.read" })
	w = execute(t, h, `{order(id:"`+fixtureID+`"){lines{sku product{name}}}}`, nil)
	if f.calls[1] != 0 || !strings.Contains(w.Body.String(), "FORBIDDEN") {
		t.Fatal(f.calls, w.Body)
	}
}

func TestPaginationSignedTenantCursorAndFreshness(t *testing.T) {
	f := newFixture(1)
	h := testHandler(t, f, nil)
	query := `query($cursor:String){orders(first:2,after:$cursor){edges{cursor node{id}}pageInfo{endCursor hasNextPage}}}`
	seen := []string{}
	cursor := ""
	for page := 0; page < 2; page++ {
		vars := map[string]any{}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		data := successful(t, execute(t, h, query, vars))
		var conn struct {
			Edges    []struct{ Node struct{ ID string } }
			PageInfo struct {
				EndCursor   string
				HasNextPage bool
			}
		}
		json.Unmarshal(data["orders"], &conn)
		for _, e := range conn.Edges {
			seen = append(seen, e.Node.ID)
		}
		if conn.PageInfo.HasNextPage != (page == 0) {
			t.Fatal(conn)
		}
		cursor = conn.PageInfo.EndCursor
		h = testHandler(t, f, nil)
	}
	if len(seen) != 4 || !sort.StringsAreSorted(seen) || seen[0] == seen[2] {
		t.Fatal(seen)
	}
	for _, c := range []string{cursor + "x", cursorCodec{[]byte(strings.Repeat("k", 32))}.encode("globex", fixtureID)} {
		before := f.calls
		w := execute(t, h, query, map[string]any{"cursor": c})
		if !strings.Contains(w.Body.String(), "BAD_USER_INPUT") || f.calls != before {
			t.Fatal(f.calls, w.Body)
		}
	}
	w := execute(t, h, `{order(id:"`+fixtureID+`",minVersion:"2"){version}}`, nil)
	if !strings.Contains(w.Body.String(), "STALE_PROJECTION") {
		t.Fatal(w.Body)
	}
}

func TestRejectBeforeAnyReadAndMaskFailures(t *testing.T) {
	queries := []string{`mutation{product(sku:"p"){name}}`, `{__schema{types{name}}}`, `{orders(first:21){edges{node{id}}}}`, `query($n:Int!){orders(first:$n){edges{node{id}}}}`, `{order(id:"` + fixtureID + `",tenant:"globex"){id}}`, `{order(id:"` + fixtureID + `"){unknown}}`, `{orders(first:20){edges{node{lines{product{details{variants{code label}}}}}}}}`, `{...A} fragment A on Query{...A}`}
	for _, q := range queries {
		f := newFixture(1)
		w := execute(t, testHandler(t, f, nil), q, map[string]any{"n": -1})
		if w.Code != 400 || f.calls != [3]int{} {
			t.Fatalf("accepted %s: %d %v %s", q, w.Code, f.calls, w.Body)
		}
	}
	f := newFixture(1)
	f.failure = true
	w := execute(t, testHandler(t, f, nil), `{product(sku:"p-000"){name}}`, nil)
	if strings.Contains(w.Body.String(), "credential") || !strings.Contains(w.Body.String(), "UNAVAILABLE") {
		t.Fatal(w.Body)
	}
}

func TestDecimalPrecisionCompatibilityAndProtocol(t *testing.T) {
	f := newFixture(1)
	f.orders[0].Version = 9007199254740993
	h := testHandler(t, f, nil)
	w := execute(t, h, `{order(id:"`+fixtureID+`"){version}product(sku:"p-000"){unitPriceMinor priceMinor}}`, nil)
	successful(t, w)
	if !strings.Contains(w.Body.String(), `"9007199254740993"`) || strings.Count(w.Body.String(), `"1000"`) != 2 {
		t.Fatal(w.Body)
	}
	for _, body := range []string{`[]`, `{"query":"{__typename}","extra":true}`, `{"query":"{__typename}"} {}`} {
		r := httptest.NewRequest("POST", "/v1/graphql", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+fixtureToken)
		out := httptest.NewRecorder()
		h.ServeHTTP(out, r)
		if out.Code != 400 {
			t.Fatal(out.Code)
		}
	}
	r := httptest.NewRequest("POST", "/v1/graphql", strings.NewReader(`{}`))
	out := httptest.NewRecorder()
	h.ServeHTTP(out, r)
	if out.Code != 401 {
		t.Fatal(out.Code)
	}
}
