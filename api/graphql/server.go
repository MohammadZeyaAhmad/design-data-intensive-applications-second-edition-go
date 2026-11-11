// Package graphql composes published read contracts without importing private
// Catalog or Ordering implementations. GraphQL describes API selection, not a
// storage model or a command authorization protocol.
package graphql

import (
	"context"
	_ "embed"
	"errors"
	"net/http"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	engine "github.com/graph-gophers/graphql-go"
	gqlerrors "github.com/graph-gophers/graphql-go/errors"
)

//go:embed schema.graphql
var Schema string

type FieldScope struct{ Tenant, Kind, ID, Permission string }
type Options struct {
	Orders         ordering.ProjectionReader
	Catalog        catalog.ProductReader
	TenantKeys     map[[32]byte]string
	CursorKey      []byte
	AuthorizeField func(context.Context, FieldScope) bool
}
type scopeKey struct{}
type requestScope struct {
	tenant   string
	options  Options
	codec    cursorCodec
	orders   *batch[ordering.Projection]
	products *batch[catalog.ProductView]
	details  *batch[catalog.DetailView]
}

func request(ctx context.Context) *requestScope {
	s, _ := ctx.Value(scopeKey{}).(*requestScope)
	return s
}
func newScope(tenant string, o Options) *requestScope {
	s := &requestScope{tenant: tenant, options: o, codec: cursorCodec{o.CursorKey}}
	s.orders = newBatch(func(ctx context.Context, ids []string) ([]ordering.Projection, error) {
		return o.Orders.ReadOrderProjections(ctx, tenant, ids)
	}, func(p ordering.Projection) (string, bool) { return p.ID, validProjection(p, tenant) })
	s.products = newBatch(func(ctx context.Context, ids []string) ([]catalog.ProductView, error) {
		return o.Catalog.ReadProducts(ctx, tenant, ids)
	}, func(p catalog.ProductView) (string, bool) {
		return p.SKU, p.TenantID == tenant && skuID.MatchString(p.SKU)
	})
	s.details = newBatch(func(ctx context.Context, ids []string) ([]catalog.DetailView, error) {
		return o.Catalog.ReadProductDetails(ctx, tenant, ids)
	}, func(p catalog.DetailView) (string, bool) {
		return p.SKU, p.TenantID == tenant && p.Brand.TenantID == tenant && skuID.MatchString(p.SKU) && skuID.MatchString(p.Brand.ID) && len(p.Variants) <= 32 && len(p.Categories) <= 16
	})
	return s
}
func (s *requestScope) permit(ctx context.Context, kind, id, permission string) error {
	if auth.Tenant(ctx) != s.tenant {
		return forbidden()
	}
	if s.options.AuthorizeField != nil && !s.options.AuthorizeField(ctx, FieldScope{s.tenant, kind, id, permission}) {
		return forbidden()
	}
	return nil
}

func New(o Options) (http.Handler, error) {
	if o.Orders == nil || o.Catalog == nil || len(o.CursorKey) < 32 || len(o.CursorKey) > 256 {
		return nil, errors.New("GraphQL requires read contracts and a persistent cursor key")
	}
	o.CursorKey = append([]byte(nil), o.CursorKey...)
	schema, e := engine.ParseSchema(Schema, &resolver{}, engine.MaxDepth(MaxDepth), engine.MaxSelections(MaxSelections), engine.MaxQueryLength(MaxQueryBytes), engine.MaxParallelism(8), engine.OverlapValidationLimit(2048), engine.DisableIntrospection())
	if e != nil {
		return nil, e
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			httpx.Problem(w, 405, "method_not_allowed")
			return
		}
		var body struct {
			Query         string         `json:"query"`
			OperationName string         `json:"operationName"`
			Variables     map[string]any `json:"variables"`
		}
		if !httpx.Decode(w, r, &body) {
			return
		}
		if _, e := Analyze(body.Query, body.OperationName, body.Variables); e != nil {
			q := e.(*publicError)
			httpx.JSON(w, 400, &engine.Response{Errors: []*gqlerrors.QueryError{{Message: q.message, Extensions: q.Extensions()}}})
			return
		}
		tenant := auth.Tenant(r.Context())
		ctx := context.WithValue(r.Context(), scopeKey{}, newScope(tenant, o))
		result := schema.Exec(ctx, body.Query, body.OperationName, body.Variables)
		result.Extensions = map[string]any{"consistency": map[string]string{"orders": "eventual projection", "catalog": "current data; separate snapshots"}}
		status := 200
		if len(result.Data) == 0 && len(result.Errors) > 0 {
			status = 400
		}
		if r.Context().Err() != nil {
			status = 503
		}
		w.Header().Set("X-Consistency", "composed")
		httpx.JSON(w, status, result)
	})
	return httpx.Guard(auth.Middleware(o.TenantKeys)(handler), nil), nil
}
