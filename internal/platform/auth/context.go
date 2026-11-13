package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

var ErrUnauthorized = errors.New("unauthorized")
var ErrInvalidCorrelation = errors.New("invalid correlation")

// Authenticate creates server-owned command metadata for an authenticated tenant.
// Adapters must reject caller-supplied actor/request/causation metadata first.
func Authenticate(ctx context.Context, keys map[[32]byte]string, authorization, correlation string) (context.Context, envelope.Metadata, error) {
	token, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok || len(token) > 256 {
		return ctx, envelope.Metadata{}, ErrUnauthorized
	}
	tenant, ok := keys[sha256.Sum256([]byte(token))]
	if !ok {
		return ctx, envelope.Metadata{}, ErrUnauthorized
	}
	if correlation != "" && !envelope.ValidID(correlation) {
		return ctx, envelope.Metadata{}, ErrInvalidCorrelation
	}
	id, err := identity.NewID()
	if err != nil {
		return ctx, envelope.Metadata{}, err
	}
	if correlation == "" {
		correlation = id
	}
	m := envelope.Metadata{Version: 1, Actor: "tenant-key:" + tenant, CorrelationID: correlation, CausationID: id, OriginRequestID: id}
	ctx = context.WithValue(ctx, tenantKey{}, tenant)
	return envelope.With(ctx, m), m, nil
}
