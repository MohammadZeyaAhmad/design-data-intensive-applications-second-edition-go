package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/telemetry/otlpfixture"
	"go.opentelemetry.io/otel/trace"
)

const fixtureToken = "metrics-only-fixture-credential-at-least-32-characters"

func newRuntime(t *testing.T, endpoint string, durable bool) *Runtime {
	t.Helper()
	r, err := New(Options{Enabled: true, Durable: durable, Service: "test", Version: "development", Endpoint: endpoint, Token: fixtureToken, Sample: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}
func TestHTTPWireExportPrivacyCardinalityAndPrivateMetrics(t *testing.T) {
	receiver := otlpfixture.New(512)
	collector := httptest.NewServer(receiver)
	defer collector.Close()
	r := newRuntime(t, collector.URL, false)
	var logs bytes.Buffer
	logger := NewLogger(&logs, "test")
	app := r.HTTP(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if sc := trace.SpanContextFromContext(req.Context()); !sc.IsValid() {
			t.Error("no real SDK span")
		}
		Logger(req.Context()).InfoContext(req.Context(), "SECRET_MESSAGE", "password", "SECRET_PASSWORD", "error", errors.New("SECRET_ERROR"), "stats", map[string]string{"tenant": "SECRET_TENANT"})
		_, _ = io.Copy(io.Discard, req.Body)
		w.WriteHeader(204)
	}))
	for i := range 40 {
		req := httptest.NewRequest("POST", "/v1/catalog/products/PRIVATE_SKU_"+strings.Repeat("x", i)+"?token=SECRET_QUERY", strings.NewReader("SECRET_BODY"))
		req = req.WithContext(WithLogger(req.Context(), logger))
		req.Header.Set("Authorization", "Bearer SECRET_BEARER")
		req.Header.Set("Baggage", "user=SECRET_BAGGAGE")
		req.Header.Set("Tracestate", "vendor=SECRET_STATE")
		req.Header.Set("Traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
		w := httptest.NewRecorder()
		app.ServeHTTP(w, req)
		if w.Code != 204 {
			t.Fatal(w.Code)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	records, rejected := receiver.Snapshot()
	if len(records) != 40 || rejected != 0 {
		t.Fatal(len(records), rejected)
	}
	for _, record := range records {
		if record.Span.Name != "http.request" || len(record.Span.TraceId) != 16 || record.Span.ParentSpanId[0] != 0x22 {
			t.Fatal(record.Span)
		}
	}
	encoded, _ := json.Marshal(records)
	for _, forbidden := range []string{"SECRET_", "PRIVATE_SKU_", "password", "Authorization", "Baggage", "Tracestate"} {
		if bytes.Contains(encoded, []byte(forbidden)) || strings.Contains(logs.String(), forbidden) {
			t.Fatal("private material reached telemetry", forbidden)
		}
	}
	metricFamilies, err := r.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range metricFamilies {
		if family.GetName() == "atlas_http_requests_total" && len(family.Metric) != 1 {
			t.Fatal("resource IDs created metric series", len(family.Metric))
		}
	}
	request := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	r.MetricsHandler().ServeHTTP(w, request)
	if w.Code != 401 {
		t.Fatal("metrics accepted a missing credential")
	}
	request.Header.Set("Authorization", "Bearer "+fixtureToken)
	w = httptest.NewRecorder()
	r.MetricsHandler().ServeHTTP(w, request)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `route="/v1/catalog/products/{sku}"`) {
		t.Fatal(w.Code)
	}
}
func TestDurableTraceGateAndUnavailableExporterPreserveBusinessResponse(t *testing.T) {
	receiver := otlpfixture.New(64)
	collector := httptest.NewServer(receiver)
	r := newRuntime(t, collector.URL, true)
	var saved envelope.Metadata
	app := r.HTTP(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		saved, _ = envelope.Record(req.Context(), "system:internal", "33333333-3333-4333-8333-333333333333")
		w.WriteHeader(201)
	}))
	w := httptest.NewRecorder()
	app.ServeHTTP(w, httptest.NewRequest("POST", "/v1/orders", nil))
	if w.Code != 201 || !envelope.ValidTraceParent(saved.TraceParent) {
		t.Fatal(w.Code, saved)
	}
	collector.Close()
	w = httptest.NewRecorder()
	app.ServeHTTP(w, httptest.NewRequest("POST", "/v1/orders", nil))
	if w.Code != 201 {
		t.Fatal("exporter outage changed business status")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Flush(ctx); err == nil {
		t.Fatal("failed export was not observable")
	}
	if r.failures.Load() == 0 {
		t.Fatal("export failures not counted")
	}
	off := newRuntime(t, "", false)
	w = httptest.NewRecorder()
	off.HTTP(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		m, _ := envelope.Record(req.Context(), "system:internal", "33333333-3333-4333-8333-333333333333")
		if m.TraceParent != "" {
			t.Error("producer gate leaked new metadata")
		}
	})).ServeHTTP(w, httptest.NewRequest("POST", "/v1/orders", nil))
}
func TestTraceParentAndConfigurationBounds(t *testing.T) {
	valid := "00-11111111111111111111111111111111-2222222222222222-01"
	if Parent(ParseParent(valid)) != valid {
		t.Fatal("trace context changed")
	}
	for _, value := range []string{"", valid + "-secret", strings.Replace(valid, "11111111111111111111111111111111", strings.Repeat("0", 32), 1), strings.Replace(valid, "-01", "-03", 1)} {
		if ParseParent(value).IsValid() {
			t.Fatal("invalid trace accepted")
		}
	}
	for _, o := range []Options{{Enabled: true, Service: "api", Sample: 2}, {Enabled: true, Durable: true, Service: "api", Sample: 1}, {Enabled: true, Service: "api", Sample: 1, Address: "0.0.0.0:9090", Token: "short"}, {Service: "api", Sample: 1, Endpoint: "http://user:secret@localhost:4318"}} {
		if o.Validate() == nil {
			t.Fatal("unsafe configuration accepted")
		}
	}
}
