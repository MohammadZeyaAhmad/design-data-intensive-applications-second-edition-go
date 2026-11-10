package auth

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/operator"
)

func TestAuthenticatedProvenanceAndReservedHeaders(t *testing.T) {
	const credential = "auth-unit-test-key-with-at-least-32-characters"
	const correlation = "11111111-1111-4111-8111-111111111111"
	var captured context.Context
	h := Middleware(map[[32]byte]string{sha256.Sum256([]byte(credential)): "acme"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { captured = r.Context(); w.WriteHeader(204) }))
	for _, headers := range []map[string]string{{"X-Actor-ID": "operator:local"}, {"X-Causation-ID": correlation}, {"X-Request-ID": correlation}, {"X-Correlation-ID": "unbounded-or-malformed-value"}} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+credential)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 || captured != nil {
			t.Fatal("untrusted metadata reached handler", w.Code)
		}
	}
	var prior string
	for range 2 {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+credential)
		r.Header.Set("X-Correlation-ID", correlation)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		m := envelope.From(captured)
		if w.Code != 204 || m.Actor != "tenant-key:acme" || m.CorrelationID != correlation || m.OriginRequestID != w.Header().Get("X-Request-ID") || m.OriginRequestID == prior || Tenant(captured) != "acme" {
			t.Fatal("invalid request lineage", m, w.Code)
		}
		if operator.Require(captured, "acme") == nil {
			t.Fatal("tenant metadata granted operator access")
		}
		prior = m.OriginRequestID
	}
}
