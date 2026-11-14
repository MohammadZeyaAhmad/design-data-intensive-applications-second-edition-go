package telemetry

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Runtime struct {
	Options        Options
	Registry       *prometheus.Registry
	provider       *sdk.TracerProvider
	tracer         trace.Tracer
	Requests       *prometheus.CounterVec
	Observed       *prometheus.CounterVec
	Unavailable    *prometheus.CounterVec
	Latency        *prometheus.HistogramVec
	SQL            *prometheus.CounterVec
	Steps          *prometheus.CounterVec
	AuditEnabled   prometheus.Gauge
	AuditUp        prometheus.Gauge
	AuditAt        prometheus.Gauge
	Lag            prometheus.Gauge
	Pending        prometheus.Gauge
	Integrity      prometheus.Gauge
	ExportFailures prometheus.Counter
	Exported       prometheus.Counter
	failures       atomic.Int64
	unavailable    atomic.Bool
	mu             sync.Mutex
	servers        []*http.Server
	closers        []func(context.Context) error
}
type runtimeKey struct{}

func (r *Runtime) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, runtimeKey{}, r)
}
func From(ctx context.Context) *Runtime { r, _ := ctx.Value(runtimeKey{}).(*Runtime); return r }
func New(o Options) (*Runtime, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	r := &Runtime{Options: o, Registry: prometheus.NewRegistry(), tracer: noop.NewTracerProvider().Tracer("atlas")}
	r.Requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "atlas_http_requests_total", Help: "Completed HTTP requests by finite route, method and status class."}, []string{"route", "method", "status"})
	r.Observed = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "atlas_http_observed_requests_total", Help: "Completed HTTP requests without a status split, used as the SLI denominator."}, []string{"route", "method"})
	r.Unavailable = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "atlas_http_unavailable_total", Help: "Known-route responses with server failure or rate exhaustion."}, []string{"route", "method"})
	r.Latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "atlas_http_request_duration_seconds", Help: "End-to-end handler duration including admission.", Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}}, []string{"route", "method"})
	r.SQL = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "atlas_sql_operations_total", Help: "SQL completions without statement text or parameters."}, []string{"operation", "outcome"})
	r.Steps = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "atlas_workflow_steps_total", Help: "Checkout steps by fixed stage and outcome."}, []string{"stage", "outcome"})
	r.AuditEnabled = prometheus.NewGauge(prometheus.GaugeOpts{Name: "atlas_audit_enabled", Help: "One only for processes that own the independent source/view audit."})
	r.AuditUp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "atlas_audit_up", Help: "One when the latest independent integrity/freshness audit succeeded."})
	r.AuditAt = prometheus.NewGauge(prometheus.GaugeOpts{Name: "atlas_audit_last_success_timestamp_seconds", Help: "Wall-clock time of the latest completed audit."})
	r.Lag = prometheus.NewGauge(prometheus.GaugeOpts{Name: "atlas_consumer_oldest_pending_seconds", Help: "Age of the oldest unprocessed Inventory source event."})
	r.Pending = prometheus.NewGauge(prometheus.GaugeOpts{Name: "atlas_consumer_pending", Help: "Unprocessed Inventory source events, including quarantined records."})
	r.Integrity = prometheus.NewGauge(prometheus.GaugeOpts{Name: "atlas_integrity_mismatches", Help: "Inventory source/view value mismatches at the same version."})
	r.ExportFailures = prometheus.NewCounter(prometheus.CounterOpts{Name: "atlas_trace_export_failures_total", Help: "Failed bounded OTLP export attempts; payloads and endpoint errors are suppressed."})
	r.Exported = prometheus.NewCounter(prometheus.CounterOpts{Name: "atlas_trace_exported_spans_total", Help: "Spans acknowledged by the configured OTLP endpoint."})
	r.Registry.MustRegister(r.Requests, r.Observed, r.Unavailable, r.Latency, r.SQL, r.Steps, r.AuditEnabled, r.AuditUp, r.AuditAt, r.Lag, r.Pending, r.Integrity, r.ExportFailures, r.Exported)
	r.Registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	if o.Enabled && o.Endpoint != "" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(o.Endpoint), otlptracehttp.WithTimeout(500*time.Millisecond), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		if err != nil {
			return nil, errors.New("could not initialize trace exporter")
		}
		processor := sdk.NewBatchSpanProcessor(&safeExporter{target: exporter, runtime: r}, sdk.WithMaxQueueSize(1024), sdk.WithMaxExportBatchSize(128), sdk.WithBatchTimeout(200*time.Millisecond), sdk.WithExportTimeout(500*time.Millisecond))
		limits := sdk.NewSpanLimits()
		limits.AttributeCountLimit = 16
		limits.EventCountLimit = 0
		limits.LinkCountLimit = 8
		r.provider = sdk.NewTracerProvider(sdk.WithSpanProcessor(processor), sdk.WithResource(resource.NewSchemaless(attribute.String("service.name", "atlas-"+o.Service), attribute.String("service.version", o.Version))), sdk.WithSampler(sdk.ParentBased(sdk.TraceIDRatioBased(o.Sample))), sdk.WithSpanLimits(limits))
		r.tracer = r.provider.Tracer("atlas/operations")
	}
	return r, nil
}

type safeExporter struct {
	target  sdk.SpanExporter
	runtime *Runtime
}

func (e *safeExporter) ExportSpans(ctx context.Context, spans []sdk.ReadOnlySpan) error {
	if err := e.target.ExportSpans(ctx, spans); err != nil {
		e.runtime.ExportFailures.Inc()
		e.runtime.failures.Add(1)
		e.runtime.unavailable.Store(true)
		return nil
	}
	e.runtime.unavailable.Store(false)
	e.runtime.Exported.Add(float64(len(spans)))
	return nil
}
func (e *safeExporter) Shutdown(ctx context.Context) error {
	if err := e.target.Shutdown(ctx); err != nil {
		return errors.New("trace exporter shutdown failed")
	}
	return nil
}
func (r *Runtime) Flush(ctx context.Context) error {
	if r.provider != nil {
		if err := r.provider.ForceFlush(ctx); err != nil {
			return errors.New("trace flush timed out")
		}
	}
	if r.unavailable.Load() {
		return errors.New("trace export unavailable")
	}
	return nil
}
func (r *Runtime) StopBackground(ctx context.Context) error {
	r.mu.Lock()
	closers := r.closers
	r.closers = nil
	r.mu.Unlock()
	var errs []error
	for _, close := range closers {
		errs = append(errs, close(ctx))
	}
	return errors.Join(errs...)
}
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	servers, closers := r.servers, r.closers
	r.servers = nil
	r.closers = nil
	r.mu.Unlock()
	var errs []error
	for _, close := range closers {
		errs = append(errs, close(ctx))
	}
	for _, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
			errs = append(errs, errors.New("metrics drain failed"))
		}
	}
	if r.provider != nil {
		errs = append(errs, r.provider.Shutdown(ctx))
	}
	return errors.Join(errs...)
}
func (r *Runtime) MetricsHandler() http.Handler {
	inner := promhttp.HandlerFor(r.Registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2, Timeout: 2 * time.Second})
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if req.Method != "GET" || req.URL.Path != "/metrics" {
			http.NotFound(w, req)
			return
		}
		expected := "Bearer " + r.Options.Token
		actual := req.Header.Values("Authorization")
		if len(r.Options.Token) < 32 || len(actual) != 1 || subtle.ConstantTimeCompare([]byte(actual[0]), []byte(expected)) != 1 {
			w.WriteHeader(401)
			return
		}
		inner.ServeHTTP(w, req)
	})
}
func (r *Runtime) Listen() error {
	if !r.Options.Enabled || r.Options.Address == "" {
		return nil
	}
	listener, err := net.Listen("tcp", r.Options.Address)
	if err != nil {
		return errors.New("private metrics listener unavailable")
	}
	server := &http.Server{Handler: r.MetricsHandler(), ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 4096}
	r.mu.Lock()
	r.servers = append(r.servers, server)
	r.mu.Unlock()
	go func() { _ = server.Serve(listener) }()
	return nil
}
func (r *Runtime) start(ctx context.Context, name string, kind trace.SpanKind) (context.Context, trace.Span) {
	ctx, span := r.tracer.Start(ctx, name, trace.WithSpanKind(kind))
	ctx = r.Context(ctx)
	if r.Options.Durable {
		ctx = envelope.WithTrace(ctx, Parent(span.SpanContext()))
	}
	return ctx, span
}
func Parent(sc trace.SpanContext) string {
	if !sc.IsValid() {
		return ""
	}
	flags := "00"
	if sc.IsSampled() {
		flags = "01"
	}
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-" + flags
}
func ParseParent(value string) trace.SpanContext {
	if !envelope.ValidTraceParent(value) {
		return trace.SpanContext{}
	}
	tid, _ := trace.TraceIDFromHex(value[3:35])
	sid, _ := trace.SpanIDFromHex(value[36:52])
	flags := trace.TraceFlags(0)
	if value[54] == '1' {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: flags, Remote: true})
}
func Producer(ctx context.Context, name string) (context.Context, func()) {
	r := From(ctx)
	if r == nil || !r.Options.Enabled {
		return ctx, func() {}
	}
	if name != "ordering.checkout.publish" && name != "ordering.checkout.dispatch" {
		name = "messaging.publish"
	}
	ctx, span := r.start(ctx, name, trace.SpanKindProducer)
	return ctx, func() { span.End() }
}
func Consumer(ctx context.Context, parent, name, stage string) (context.Context, func(error)) {
	r := From(ctx)
	if r == nil || !r.Options.Enabled {
		return ctx, func(error) {}
	}
	if name != "checkout.step" && name != "inventory.projection.apply" {
		name = "messaging.consume"
	}
	if sc := ParseParent(parent); sc.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	}
	ctx, span := r.start(ctx, name, trace.SpanKindConsumer)
	stage = Stage(stage)
	span.SetAttributes(attribute.String("workflow.stage", stage))
	return ctx, func(err error) {
		outcome := "ok"
		if err != nil {
			outcome = "failed"
			span.SetStatus(codes.Error, "operation_failed")
		}
		if name == "checkout.step" {
			r.Steps.WithLabelValues(stage, outcome).Inc()
		}
		span.End()
	}
}
func Stage(stage string) string {
	switch stage {
	case "reserving", "capturing", "allocating", "confirming", "compensating", "confirmed", "failed", "cancelled":
		return stage
	default:
		return "other"
	}
}
