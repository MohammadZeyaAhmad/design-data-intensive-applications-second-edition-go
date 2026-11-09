package httpapi

import (
	"context"
	"crypto/sha256"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
)

const testKey = "unit-test-only-key-with-at-least-32-characters"

type readStore struct{ ports.Store }

func (readStore) Load(_ context.Context, tenant, id string) (domain.State, error) {
	return domain.State{TenantID: tenant, ID: id, Version: 1, Status: domain.Placed, Lines: []domain.Line{{SKU: "BOOK", Quantity: 1}}}, nil
}
func TestOrderHTTPRejectsAmbiguousInputBeforePersistence(t *testing.T) {
	h := New(Options{Store: readStore{}, TenantKeys: map[[32]byte]string{sha256.Sum256([]byte(testKey)): "acme"}})
	for _, tc := range []struct{ path, body string }{
		{"/v1/orders", `{"lines":[{"sku":"BOOK","quantity":1}],"tenant_id":"globex"}`},
		{"/v1/orders", `{"lines":[{"sku":"BOOK","quantity":0}]}`},
		{"/v1/orders/72f44666-e594-4cd8-aa66-f44e6c2dd387/amend", `{"lines":[{"sku":"BOOK","quantity":1}]}`},
		{"/v1/orders/not-a-uuid/cancel", `{"expected_version":1,"reason":"customer_requested"}`},
	} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+testKey)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "key")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("%s returned %d: %s", tc.path, w.Code, w.Body)
		}
	}
	r := httptest.NewRequest("GET", "/v1/orders/72f44666-e594-4cd8-aa66-f44e6c2dd387", nil)
	r.Header.Set("Authorization", "Bearer "+testKey)
	r.Header.Set("X-Tenant-ID", "globex")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"tenant_id":"acme"`) {
		t.Fatal("caller selected another tenant")
	}
}
