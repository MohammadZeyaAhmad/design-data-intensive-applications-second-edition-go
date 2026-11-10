package auth

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
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
			key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || len(key) > 256 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				httpx.Problem(w, 401, "unauthorized")
				return
			}
			tenant, ok := keys[sha256.Sum256([]byte(key))]
			if !ok {
				w.Header().Set("WWW-Authenticate", "Bearer")
				httpx.Problem(w, 401, "unauthorized")
				return
			}
			if r.Header.Get("X-Actor-ID") != "" || r.Header.Get("X-Causation-ID") != "" || r.Header.Get("X-Request-ID") != "" {
				httpx.Problem(w, 400, "server_owned_metadata")
				return
			}
			correlation := r.Header.Get("X-Correlation-ID")
			if len(r.Header.Values("X-Correlation-ID")) > 1 || correlation != "" && !envelope.ValidID(correlation) {
				httpx.Problem(w, 400, "invalid_correlation_id")
				return
			}
			requestID, err := identity.NewID()
			if err != nil {
				httpx.Problem(w, 503, "identity_unavailable")
				return
			}
			if correlation == "" {
				correlation = requestID
			}
			m := envelope.Metadata{Version: 1, Actor: "tenant-key:" + tenant, CorrelationID: correlation, CausationID: requestID, OriginRequestID: requestID}
			w.Header().Set("X-Request-ID", requestID)
			w.Header().Set("X-Correlation-ID", correlation)
			ctx := context.WithValue(r.Context(), tenantKey{}, tenant)
			next.ServeHTTP(w, r.WithContext(envelope.With(ctx, m)))
		})
	}
}
