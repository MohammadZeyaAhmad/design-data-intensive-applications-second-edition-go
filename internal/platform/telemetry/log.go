package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
)

// All arbitrary messages and attribute values are redacted by default. Adding a
// useful field requires an explicit bounded representation here.
var hexTrace = regexp.MustCompile(`^([0-9a-f]{16}|[0-9a-f]{32})$`)

type logHandler struct {
	inner   slog.Handler
	attrs   []slog.Attr
	service string
}

func NewLogger(out io.Writer, service string) *slog.Logger {
	if !slug.MatchString(service) {
		service = "atlas"
	}
	return slog.New(&logHandler{inner: slog.NewJSONHandler(out, nil), service: service})
}
func (h *logHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.inner.Enabled(ctx, l) }
func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	message := "application_log"
	switch r.Message {
	case "http.request", "request panic", "atlas stopped", "migrations applied", "database and schema ready", "Atlas API started", "Atlas inventory, ordering, checkout and fulfillment workers started", "catalog cache stopped", "catalog cache drain failed":
		message = r.Message
	}
	clean := slog.NewRecord(r.Time, r.Level, message, r.PC)
	clean.AddAttrs(slog.String("service", h.service))
	visit := func(a slog.Attr) bool {
		switch a.Key {
		case "cache_hits", "cache_misses", "cache_fallbacks", "cache_loads", "cache_invalidations_failed":
			if a.Value.Kind() == slog.KindInt64 && a.Value.Int64() >= 0 {
				clean.AddAttrs(a)
			}
		case "status", "duration_ms":
			if a.Value.Kind() == slog.KindInt64 && a.Value.Int64() >= 0 && a.Value.Int64() <= 1000000 {
				clean.AddAttrs(a)
			}
		case "method":
			if a.Value.Kind() == slog.KindString && method(a.Value.String()) == a.Value.String() {
				clean.AddAttrs(a)
			}
		case "route":
			if a.Value.Kind() == slog.KindString {
				value := a.Value.String()
				if value == "unmatched" || strings.HasPrefix(value, "/v1/") && Route(strings.NewReplacer("{id}", "x", "{sku}", "x", "*", "x").Replace(value)) == value || value == "/healthz" || value == "/readyz" {
					clean.AddAttrs(a)
				}
			}
		case "trace_id", "span_id":
			if a.Value.Kind() == slog.KindString && hexTrace.MatchString(a.Value.String()) {
				clean.AddAttrs(a)
			}
		default:
			clean.AddAttrs(slog.Bool("redacted", true))
		}
		return true
	}
	for _, a := range h.attrs {
		visit(a)
	}
	r.Attrs(visit)
	return h.inner.Handle(ctx, clean)
}
func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copy := *h
	copy.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &copy
}
func (h *logHandler) WithGroup(string) slog.Handler { return h }

type loggerKey struct{}

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}
func Logger(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return logger
	}
	return NewLogger(os.Stdout, "atlas")
}
