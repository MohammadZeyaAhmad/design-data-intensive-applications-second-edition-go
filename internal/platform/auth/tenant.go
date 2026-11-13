package auth

import (
	"context"
	"errors"
	"net/http"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
)

type tenantKey struct{}

func Tenant(ctx context.Context) string { tenant, _ := ctx.Value(tenantKey{}).(string); return tenant }
func Middleware(input map[[32]byte]string) func(http.Handler) http.Handler {
	keys := make(map[[32]byte]string, len(input))
	for key, tenant := range input {
		keys[key] = tenant
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(r.Header.Values("Authorization")) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				httpx.Problem(w, 401, "unauthorized")
				return
			}
			for _, key := range []string{"X-Actor-ID", "X-Causation-ID", "X-Request-ID"} {
				if len(r.Header.Values(key)) > 0 {
					httpx.Problem(w, 400, "server_owned_metadata")
					return
				}
			}
			correlation := r.Header.Get("X-Correlation-ID")
			if len(r.Header.Values("X-Correlation-ID")) > 1 || correlation != "" && !envelope.ValidID(correlation) {
				httpx.Problem(w, 400, "invalid_correlation_id")
				return
			}
			ctx, m, err := Authenticate(r.Context(), keys, r.Header.Get("Authorization"), correlation)
			if err != nil {
				if errors.Is(err, ErrUnauthorized) {
					w.Header().Set("WWW-Authenticate", "Bearer")
					httpx.Problem(w, 401, "unauthorized")
				} else {
					httpx.Problem(w, 503, "identity_unavailable")
				}
				return
			}
			w.Header().Set("X-Request-ID", m.OriginRequestID)
			w.Header().Set("X-Correlation-ID", m.CorrelationID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
