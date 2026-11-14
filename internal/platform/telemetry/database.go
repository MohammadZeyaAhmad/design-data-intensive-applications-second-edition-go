package telemetry

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type querySpan struct{}
type query struct {
	span      trace.Span
	operation string
}
type DBTracer struct{ Runtime *Runtime }

func Operation(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "OTHER"
	}
	switch first := strings.ToUpper(fields[0]); first {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT", "ROLLBACK", "CREATE", "ALTER", "DROP", "WITH", "SHOW", "SET":
		return first
	default:
		return "OTHER"
	}
}
func (t DBTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(suppressKey{}) == true || !t.Runtime.Options.Enabled {
		return ctx
	}
	operation := Operation(data.SQL)
	span := trace.SpanFromContext(context.Background())
	if trace.SpanContextFromContext(ctx).IsValid() {
		ctx, span = t.Runtime.start(ctx, "sql.query", trace.SpanKindClient)
		span.SetAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.operation.name", operation))
	}
	return context.WithValue(ctx, querySpan{}, query{span: span, operation: operation})
}
func (t DBTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	q, ok := ctx.Value(querySpan{}).(query)
	if !ok {
		return
	}
	defer q.span.End()
	outcome := "ok"
	if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
		outcome = "failed"
		if errors.Is(data.Err, context.Canceled) {
			outcome = "canceled"
		}
		if errors.Is(data.Err, context.DeadlineExceeded) {
			outcome = "deadline"
		}
		q.span.SetStatus(codes.Error, "database_operation_failed")
	}
	t.Runtime.SQL.WithLabelValues(q.operation, outcome).Inc()
}
