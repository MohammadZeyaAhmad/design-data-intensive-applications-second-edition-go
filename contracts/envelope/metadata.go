// Package envelope defines provenance shared by published messages. Metadata is
// diagnostic lineage, never an authorization capability or an idempotency key.
package envelope

import (
	"context"
	"errors"
	"regexp"
)

var ErrInvalid = errors.New("invalid provenance metadata")
var uuid = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var actor = regexp.MustCompile(`^(tenant-key:[a-z][a-z0-9-]{0,63}|worker:[a-z][a-z0-9-]{0,63}|operator:local|system:internal)$`)

type Metadata struct {
	TraceParent     string `json:"traceparent,omitempty"`
	Version         int    `json:"version"`
	Actor           string `json:"actor,omitempty"`
	CorrelationID   string `json:"correlation_id,omitempty"`
	CausationID     string `json:"causation_id,omitempty"`
	OriginRequestID string `json:"origin_request_id,omitempty"`
}

func ValidID(id string) bool { return uuid.MatchString(id) }
func (m Metadata) Valid() bool {
	if m.Version == 0 {
		return m == (Metadata{})
	}
	return m.Version == 1 && (m.TraceParent == "" || ValidTraceParent(m.TraceParent)) && actor.MatchString(m.Actor) && ValidID(m.CorrelationID) && ValidID(m.CausationID) && (m.OriginRequestID == "" || ValidID(m.OriginRequestID))
}

type key struct{}

func With(ctx context.Context, m Metadata) context.Context { return context.WithValue(ctx, key{}, m) }
func From(ctx context.Context) Metadata                    { m, _ := ctx.Value(key{}).(Metadata); return m }

// Record creates a new fact's immediate cause while keeping its current actor,
// correlation and originating request. A zero envelope explicitly means legacy
// or internal work; new facts still receive a version-one envelope.
func Record(ctx context.Context, fallbackActor, cause string) (Metadata, error) {
	m := From(ctx)
	if !m.Valid() || !ValidID(cause) {
		return Metadata{}, ErrInvalid
	}
	if m.Version == 0 {
		m = Metadata{Version: 1, Actor: fallbackActor, CorrelationID: cause}
	}
	m.CausationID = cause
	if parent := TraceFrom(ctx); parent != "" {
		m.TraceParent = parent
	}
	if !m.Valid() {
		return Metadata{}, ErrInvalid
	}
	return m, nil
}

// Worker resumes the persisted parent instead of inheriting a new process's
// unrelated context. The executor is recorded separately from its initiator.
func Worker(ctx context.Context, parent Metadata, name, cause string) (context.Context, error) {
	if !parent.Valid() {
		return ctx, ErrInvalid
	}
	m, err := Record(With(ctx, parent), "worker:"+name, cause)
	if err != nil {
		return ctx, err
	}
	m.Actor = "worker:" + name
	if !m.Valid() {
		return ctx, ErrInvalid
	}
	return With(ctx, m), nil
}
