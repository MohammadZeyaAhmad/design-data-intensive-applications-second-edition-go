package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
)

const testKey = "unit-test-only-key-with-at-least-32-characters"

type readStore struct{ ports.Store }

func (readStore) Stock(_ context.Context, tenant, sku string) (domain.StockState, error) {
	return domain.StockState{TenantID: tenant, SKU: sku, OnHand: 1, Version: 1}, nil
}
func handler() http.Handler {
	return New(Options{Store: readStore{}, TenantKeys: map[[32]byte]string{sha256.Sum256([]byte(testKey)): "acme"}, Ready: func(context.Context) error { return nil }})
}

func TestUntrustedInputDoesNotReachPersistence(t *testing.T) {
	for _, tc := range []struct {
		name, body, key, content string
		status                   int
	}{
		{"missing credential", `{"sku":"BOOK","quantity":1}`, "", "application/json", 401},
		{"tenant body override", `{"sku":"BOOK","quantity":1,"tenant_id":"globex"}`, testKey, "application/json", 400},
		{"negative quantity", `{"sku":"BOOK","quantity":-1}`, testKey, "application/json", 400},
		{"extra JSON", `{"sku":"BOOK","quantity":1} {}`, testKey, "application/json", 400},
		{"numeric overflow", `{"sku":"BOOK","quantity":999999999999999999999999}`, testKey, "application/json", 400},
		{"wrong media", `{"sku":"BOOK","quantity":1}`, testKey, "text/plain", 415},
		{"oversized body", `{"sku":"` + strings.Repeat("a", 70<<10) + `","quantity":1}`, testKey, "application/json", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/reservations", strings.NewReader(tc.body))
			if tc.key != "" {
				r.Header.Set("Authorization", "Bearer "+tc.key)
			}
			r.Header.Set("Content-Type", tc.content)
			r.Header.Set("Idempotency-Key", "test")
			w := httptest.NewRecorder()
			handler().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("got %d: %s", w.Code, w.Body)
			}
		})
	}
}
func TestCredentialBindsTenant(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/stock/BOOK", nil)
	r.Header.Set("Authorization", "Bearer "+testKey)
	r.Header.Set("X-Tenant-ID", "globex")
	w := httptest.NewRecorder()
	handler().ServeHTTP(w, r)
	var s domain.StockState
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || s.TenantID != "acme" {
		t.Fatal("untrusted header changed tenant identity")
	}
}
func TestReadinessDoesNotExposeInternalErrors(t *testing.T) {
	h := New(Options{Ready: func(context.Context) error { return errors.New("private-connection-details") }})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "private") {
		t.Fatal("readiness leaked internal detail")
	}
}
