package auth

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"

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
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tenant)))
		})
	}
}
