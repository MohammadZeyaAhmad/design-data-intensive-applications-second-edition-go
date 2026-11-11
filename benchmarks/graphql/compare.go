// Package graphql compares the actual application GraphQL adapter with the
// corresponding REST reads. In-process HTTP counts are not wire latency tests.
package graphql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
)

type counterKey struct{}

func WithCounter(ctx context.Context) (context.Context, *atomic.Int64) {
	c := &atomic.Int64{}
	return context.WithValue(ctx, counterKey{}, c), c
}
func record(ctx context.Context) {
	if c, _ := ctx.Value(counterKey{}).(*atomic.Int64); c != nil {
		c.Add(1)
	}
}

type QueryTracer struct{}

func (QueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	record(ctx)
	return ctx
}
func (QueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type Line struct {
	SKU      string   `json:"sku"`
	Quantity int64    `json:"quantity"`
	Product  *Product `json:"product"`
}
type Product struct {
	SKU            string   `json:"sku"`
	Name           string   `json:"name"`
	Version        string   `json:"version"`
	Currency       string   `json:"currency"`
	UnitPriceMinor string   `json:"unitPriceMinor"`
	Details        *Details `json:"details"`
}
type Details struct {
	Revision    string `json:"revision"`
	Description string `json:"description"`
	Brand       struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"brand"`
	Categories []string `json:"categories"`
	Variants   []struct {
		Code  string `json:"code"`
		Label string `json:"label"`
	} `json:"variants"`
}
type Joined struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Status  string `json:"status"`
	Lines   []Line `json:"lines"`
}
type Sample struct {
	Lines               int    `json:"lines"`
	GraphQLReads        int64  `json:"graphql_reads"`
	RESTReads           int64  `json:"rest_reads"`
	GraphQLHTTPRequests int    `json:"graphql_http_requests"`
	RESTHTTPRequests    int    `json:"rest_http_requests"`
	GraphQLBytes        int    `json:"graphql_response_bytes"`
	RESTBytes           int    `json:"rest_response_bytes"`
	AnswerDigest        string `json:"answer_digest"`
}
type Report struct {
	Passed      bool     `json:"passed"`
	Mode        string   `json:"mode"`
	Samples     []Sample `json:"samples"`
	Definitions []string `json:"definitions"`
}

func queryDocument() (string, error) {
	b, e := os.ReadFile("labs/ch03/graphql/order-composition.graphql")
	return string(b), e
}

// CompositionQuery also works from tests/integration with an explicit repo root.
func CompositionQuery(root string) (string, error) {
	b, e := os.ReadFile(root + "/labs/ch03/graphql/order-composition.graphql")
	return string(b), e
}
func call(ctx context.Context, h http.Handler, token, method, path string, body any) ([]byte, error) {
	var payload []byte
	if body != nil {
		var e error
		payload, e = json.Marshal(body)
		if e != nil {
			return nil, e
		}
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(payload)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		return nil, fmt.Errorf("comparison HTTP status %d", w.Code)
	}
	return w.Body.Bytes(), nil
}
func Compare(ctx context.Context, h http.Handler, token, id, query string, lines int) (Sample, error) {
	sample := Sample{Lines: lines, GraphQLHTTPRequests: 1, RESTHTTPRequests: 1 + 2*lines}
	gctx, gcount := WithCounter(ctx)
	wire, e := call(gctx, h, token, "POST", "/v1/graphql", map[string]any{"query": query, "variables": map[string]any{"id": id}})
	if e != nil {
		return sample, e
	}
	sample.GraphQLBytes = len(wire)
	sample.GraphQLReads = gcount.Load()
	var graph struct {
		Data   struct{ Order Joined }
		Errors []any
	}
	if json.Unmarshal(wire, &graph) != nil || len(graph.Errors) > 0 || graph.Data.Order.ID != id || len(graph.Data.Order.Lines) != lines {
		return sample, fmt.Errorf("GraphQL comparison returned an incomplete answer")
	}
	rctx, rcount := WithCounter(ctx)
	wire, e = call(rctx, h, token, "GET", "/v1/orders/"+url.PathEscape(id)+"/projection", nil)
	if e != nil {
		return sample, e
	}
	sample.RESTBytes += len(wire)
	var order struct {
		ID      string
		Version int64
		Status  string
		Lines   []struct {
			SKU      string
			Quantity int64
		}
	}
	if json.Unmarshal(wire, &order) != nil || order.ID != id || len(order.Lines) != lines {
		return sample, fmt.Errorf("REST projection mismatch")
	}
	rest := Joined{ID: order.ID, Version: strconv.FormatInt(order.Version, 10), Status: order.Status, Lines: []Line{}}
	for _, line := range order.Lines {
		wire, e = call(rctx, h, token, "GET", "/v1/catalog/products/"+url.PathEscape(line.SKU), nil)
		if e != nil {
			return sample, e
		}
		sample.RESTBytes += len(wire)
		var product struct {
			SKU            string
			Name           string
			Version        int64
			Currency       string
			UnitPriceMinor int64 `json:"unit_price_minor"`
		}
		if json.Unmarshal(wire, &product) != nil {
			return sample, fmt.Errorf("REST product mismatch")
		}
		p := &Product{SKU: product.SKU, Name: product.Name, Version: strconv.FormatInt(product.Version, 10), Currency: product.Currency, UnitPriceMinor: strconv.FormatInt(product.UnitPriceMinor, 10)}
		wire, e = call(rctx, h, token, "GET", "/v1/catalog/products/"+url.PathEscape(line.SKU)+"/details", nil)
		if e != nil {
			return sample, e
		}
		sample.RESTBytes += len(wire)
		var details struct {
			Version     int64
			Description string
			BrandID     string `json:"brand_id"`
			BrandName   string `json:"brand_name"`
			Categories  []string
			Variants    []struct {
				Code  string `json:"code"`
				Label string `json:"label"`
			}
		}
		if json.Unmarshal(wire, &details) != nil {
			return sample, fmt.Errorf("REST details mismatch")
		}
		d := &Details{Revision: strconv.FormatInt(details.Version, 10), Description: details.Description, Categories: details.Categories, Variants: details.Variants}
		d.Brand.ID, d.Brand.Name = details.BrandID, details.BrandName
		p.Details = d
		rest.Lines = append(rest.Lines, Line{line.SKU, line.Quantity, p})
	}
	sample.RESTReads = rcount.Load()
	if !reflect.DeepEqual(graph.Data.Order, rest) {
		return sample, fmt.Errorf("complete GraphQL and REST answers differ")
	}
	if sample.GraphQLReads != 3 || sample.RESTReads != int64(1+2*lines) {
		return sample, fmt.Errorf("read count differs: GraphQL %d REST %d", sample.GraphQLReads, sample.RESTReads)
	}
	b, _ := json.Marshal(rest)
	sum := sha256.Sum256(b)
	sample.AnswerDigest = hex.EncodeToString(sum[:])
	return sample, nil
}
func Definitions() []string {
	return []string{
		"the actual GraphQL engine composes published Ordering projection and Catalog read contracts; production contexts never import this comparison",
		"one GraphQL request batches all products and details; baseline REST composition uses one projection plus two single-resource requests per line",
		"answers compare every selected scalar, product, normalized brand, category and ordered variant; counts alone are insufficient",
		"request-local memoization includes missing values and ownership checks; separate requests can observe later source revisions",
		"HTTP handlers execute in process, with authentication and JSON; payload counts exclude headers and are not network latency measurements",
		"native counts are pgx query executions, not protocol packets, database operator work or a universal GraphQL/REST ranking",
	}
}
