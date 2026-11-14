package envelope

import (
	"context"
	"regexp"
	"strings"
)

var traceParentPattern = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$`)

// ValidTraceParent accepts the bounded W3C version-00 subset used by Atlas.
// Baggage and tracestate never enter durable provenance.
func ValidTraceParent(value string) bool {
	return traceParentPattern.MatchString(value) && value[3:35] != strings.Repeat("0", 32) && value[36:52] != strings.Repeat("0", 16)
}

type traceKey struct{}

func WithTrace(ctx context.Context, parent string) context.Context {
	if !ValidTraceParent(parent) {
		parent = ""
	}
	return context.WithValue(ctx, traceKey{}, parent)
}
func TraceFrom(ctx context.Context) string { value, _ := ctx.Value(traceKey{}).(string); return value }
