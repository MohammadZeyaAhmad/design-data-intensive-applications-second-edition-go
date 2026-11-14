package telemetry

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Route returns only authored templates. Unknown paths and identifiers can never
// create metric series, span names or log fields.
func Route(path string) string {
	if path == "/healthz" || path == "/readyz" {
		return path
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] != "v1" {
		return "unmatched"
	}
	switch parts[1] {
	case "inventory":
		if len(parts) == 3 && parts[2] == "snapshot" {
			return "/v1/inventory/snapshot"
		}
	case "graphql":
		if len(parts) == 2 {
			return "/v1/graphql"
		}
	case "orders":
		if len(parts) == 2 {
			return "/v1/orders"
		}
		if len(parts) == 3 {
			return "/v1/orders/{id}"
		}
		if len(parts) == 4 {
			switch parts[3] {
			case "checkout", "events", "fulfillment", "cancel", "amend":
				return "/v1/orders/{id}/" + parts[3]
			}
		}
	case "checkouts":
		if len(parts) == 3 {
			return "/v1/checkouts/{id}"
		}
		if len(parts) == 4 && parts[3] == "cancel" {
			return "/v1/checkouts/{id}/cancel"
		}
	case "reservations":
		if len(parts) == 2 {
			return "/v1/reservations"
		}
		if len(parts) == 3 {
			return "/v1/reservations/{id}"
		}
		if len(parts) == 4 {
			switch parts[3] {
			case "commit", "release", "reverse":
				return "/v1/reservations/{id}/" + parts[3]
			}
		}
	case "stock":
		if len(parts) == 3 {
			return "/v1/stock/{sku}"
		}
		if len(parts) == 4 {
			switch parts[3] {
			case "receipts", "projection":
				return "/v1/stock/{sku}/" + parts[3]
			}
		}
	case "catalog":
		if len(parts) == 3 {
			switch parts[2] {
			case "quotes":
				return "/v1/catalog/quotes"
			}
		}
		if len(parts) == 4 {
			switch parts[2] {
			case "products":
				return "/v1/catalog/products/{sku}"
			case "quotes":
				return "/v1/catalog/quotes/{id}"
			case "brands":
				return "/v1/catalog/brands/{id}"
			}
		}
		if len(parts) == 5 && parts[2] == "products" && parts[4] == "details" {
			return "/v1/catalog/products/{sku}/details"
		}
	case "payments":
		if len(parts) == 3 {
			return "/v1/payments/{id}"
		}
		if len(parts) == 4 {
			switch parts[3] {
			case "capture", "close", "lookup":
				return "/v1/payments/{id}/" + parts[3]
			}
		}
	case "shipments":
		if len(parts) == 3 {
			return "/v1/shipments/{id}"
		}
	case "search":
		return "/v1/search/*"
	}
	return "unmatched"
}
func method(value string) string {
	switch value {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		return value
	default:
		return "OTHER"
	}
}

type response struct {
	http.ResponseWriter
	status int
}

func (w *response) WriteHeader(code int) {
	if code >= 200 && w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *response) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *response) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *response) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *response) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.status = 500
	return http.NewResponseController(w.ResponseWriter).Hijack()
}
func (r *Runtime) HTTP(next http.Handler) http.Handler {
	if !r.Options.Enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		began := time.Now()
		route, verb := Route(req.URL.Path), method(req.Method)
		ctx := r.Context(req.Context())
		if values := req.Header.Values("Traceparent"); len(values) == 1 {
			if sc := ParseParent(values[0]); sc.IsValid() {
				ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
			}
		}
		ctx, span := r.start(ctx, "http.request", trace.SpanKindServer)
		span.SetAttributes(attribute.String("http.route", route), attribute.String("http.request.method", verb))
		wrapped := &response{ResponseWriter: w}
		defer func() {
			code := wrapped.status
			if code == 0 {
				code = 200
			}
			status := "other"
			if code >= 100 && code < 600 {
				status = string(rune('0'+code/100)) + "xx"
			}
			elapsed := time.Since(began).Seconds()
			r.Requests.WithLabelValues(route, verb, status).Inc()
			r.Observed.WithLabelValues(route, verb).Inc()
			unavailable := r.Unavailable.WithLabelValues(route, verb)
			unavailable.Add(0)
			if code >= 500 || code == 429 {
				unavailable.Inc()
			}
			r.Latency.WithLabelValues(route, verb).Observe(elapsed)
			span.SetAttributes(attribute.Int("http.response.status_code", code))
			if code >= 500 {
				span.SetStatus(codes.Error, "http_server_failure")
			}
			span.End()
			Logger(ctx).InfoContext(ctx, "http.request", "route", route, "method", verb, "status", code, "duration_ms", time.Since(began).Milliseconds(), "trace_id", span.SpanContext().TraceID().String(), "span_id", span.SpanContext().SpanID().String())
		}()
		next.ServeHTTP(wrapped, req.WithContext(ctx))
	})
}

type Transport struct{ Base http.RoundTripper }

func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	r := From(req.Context())
	if r == nil || !r.Options.Enabled {
		return base.RoundTrip(req)
	}
	ctx, span := r.start(req.Context(), "payment_provider.request", trace.SpanKindClient)
	defer span.End()
	cloned := req.Clone(ctx)
	cloned.Header = req.Header.Clone()
	if parent := Parent(span.SpanContext()); parent != "" {
		cloned.Header.Set("Traceparent", parent)
	}
	cloned.Header.Del("Tracestate")
	cloned.Header.Del("Baggage")
	out, err := base.RoundTrip(cloned)
	if err != nil {
		span.SetStatus(codes.Error, "provider_transport_failure")
	} else {
		span.SetAttributes(attribute.Int("http.response.status_code", out.StatusCode))
		if out.StatusCode >= 500 || out.StatusCode == 429 {
			span.SetStatus(codes.Error, "provider_unavailable")
		}
	}
	return out, err
}
func TraceID(ctx context.Context) string { return trace.SpanContextFromContext(ctx).TraceID().String() }
