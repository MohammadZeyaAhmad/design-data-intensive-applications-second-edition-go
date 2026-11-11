package graphql

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"sort"
	"strings"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/graphql"
	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	ord "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
)

type memoryFixture struct {
	orders   []ord.Projection
	products []cat.ProductView
	details  []cat.DetailView
}

func (f *memoryFixture) ReadOrderProjections(ctx context.Context, tenant string, ids []string) ([]ord.Projection, error) {
	record(ctx)
	out := []ord.Projection{}
	for _, o := range f.orders {
		for _, id := range ids {
			if o.TenantID == tenant && o.ID == id {
				out = append(out, o)
			}
		}
	}
	return out, ctx.Err()
}
func (f *memoryFixture) ListOrderProjections(ctx context.Context, tenant, after string, first int) (ord.ProjectionPage, error) {
	record(ctx)
	out := ord.ProjectionPage{Nodes: []ord.Projection{}}
	for _, o := range f.orders {
		if o.TenantID == tenant && o.ID > after {
			out.Nodes = append(out.Nodes, o)
		}
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].ID < out.Nodes[j].ID })
	if len(out.Nodes) > first {
		out.HasNextPage = true
		out.Nodes = out.Nodes[:first]
	}
	return out, ctx.Err()
}
func (f *memoryFixture) ReadProducts(ctx context.Context, tenant string, ids []string) ([]cat.ProductView, error) {
	record(ctx)
	out := []cat.ProductView{}
	for _, p := range f.products {
		for _, id := range ids {
			if p.TenantID == tenant && p.SKU == id {
				out = append(out, p)
			}
		}
	}
	return out, ctx.Err()
}
func (f *memoryFixture) ReadProductDetails(ctx context.Context, tenant string, ids []string) ([]cat.DetailView, error) {
	record(ctx)
	out := []cat.DetailView{}
	for _, p := range f.details {
		for _, id := range ids {
			if p.TenantID == tenant && p.SKU == id {
				out = append(out, p)
			}
		}
	}
	return out, ctx.Err()
}
func newMemory() *memoryFixture {
	f := &memoryFixture{}
	for _, tenant := range []string{"acme", "globex"} {
		for i := 0; i < 100; i++ {
			sku := fmt.Sprintf("p-%03d", i)
			f.products = append(f.products, cat.ProductView{TenantID: tenant, SKU: sku, Name: tenant + " " + sku, UnitPriceMinor: int64(1000 + i), Currency: "USD", Version: 1})
			f.details = append(f.details, cat.DetailView{TenantID: tenant, SKU: sku, Description: "Details " + sku, Version: 1, Brand: cat.BrandView{TenantID: tenant, ID: "maker", Name: tenant + " Maker"}, Categories: []string{"devices"}, Variants: []cat.VariantView{{Code: "blue", Label: "Blue"}}})
		}
		for j, n := range []int{1, 10, 100} {
			o := ord.Projection{TenantID: tenant, ID: fmt.Sprintf("00000000-0000-0000-0000-%012d", j+1), Version: 1, Status: "placed", Lines: []ord.ProjectionLine{}}
			for i := 0; i < n; i++ {
				o.Lines = append(o.Lines, ord.ProjectionLine{SKU: fmt.Sprintf("p-%03d", i), Quantity: int64(i + 1)})
			}
			f.orders = append(f.orders, o)
		}
	}
	return f
}
func (f *memoryFixture) rest(keys map[[32]byte]string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/orders/{id}/projection", func(w http.ResponseWriter, r *http.Request) {
		rows, e := f.ReadOrderProjections(r.Context(), auth.Tenant(r.Context()), []string{r.PathValue("id")})
		if e != nil || len(rows) != 1 {
			httpx.Problem(w, 404, "not_found")
			return
		}
		o := rows[0]
		lines := []map[string]any{}
		for _, l := range o.Lines {
			lines = append(lines, map[string]any{"sku": l.SKU, "quantity": l.Quantity})
		}
		httpx.JSON(w, 200, map[string]any{"id": o.ID, "version": o.Version, "status": o.Status, "lines": lines})
	})
	mux.HandleFunc("GET /v1/catalog/products/{sku}", func(w http.ResponseWriter, r *http.Request) {
		rows, e := f.ReadProducts(r.Context(), auth.Tenant(r.Context()), []string{r.PathValue("sku")})
		if e != nil || len(rows) != 1 {
			httpx.Problem(w, 404, "not_found")
			return
		}
		p := rows[0]
		httpx.JSON(w, 200, map[string]any{"sku": p.SKU, "name": p.Name, "version": p.Version, "currency": p.Currency, "unit_price_minor": p.UnitPriceMinor})
	})
	mux.HandleFunc("GET /v1/catalog/products/{sku}/details", func(w http.ResponseWriter, r *http.Request) {
		rows, e := f.ReadProductDetails(r.Context(), auth.Tenant(r.Context()), []string{r.PathValue("sku")})
		if e != nil || len(rows) != 1 {
			httpx.Problem(w, 404, "not_found")
			return
		}
		d := rows[0]
		httpx.JSON(w, 200, map[string]any{"version": d.Version, "description": d.Description, "brand_id": d.Brand.ID, "brand_name": d.Brand.Name, "categories": d.Categories, "variants": d.Variants})
	})
	return auth.Middleware(keys)(mux)
}
func Run(ctx context.Context) (Report, error) {
	report := Report{Mode: "in-memory published-contract calls; actual GraphQL engine", Samples: []Sample{}, Definitions: Definitions()}
	f := newMemory()
	token := "graphql-comparison-disposable-key"
	keys := map[[32]byte]string{sha256.Sum256([]byte(token)): "acme"}
	graph, e := api.New(api.Options{Orders: f, Catalog: f, TenantKeys: keys, CursorKey: []byte(strings.Repeat("c", 32))})
	if e != nil {
		return report, e
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/graphql", graph)
	mux.Handle("/", f.rest(keys))
	query, e := queryDocument()
	if e != nil {
		return report, e
	}
	for i, n := range []int{1, 10, 100} {
		s, e := Compare(ctx, mux, token, fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1), query, n)
		if e != nil {
			return report, e
		}
		report.Samples = append(report.Samples, s)
	}
	report.Passed = true
	return report, nil
}
