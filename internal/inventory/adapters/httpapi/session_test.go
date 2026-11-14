package httpapi

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/readrouting"
)

type failingFence struct {
	ports.SessionReader
	verify error
}

func (f failingFence) VerifySession(context.Context, string, string) error { return f.verify }
func (f failingFence) AfterWrite(context.Context, string, string) (ports.ReadMeta, error) {
	return ports.ReadMeta{}, readrouting.ErrUnavailable
}
func TestCommittedReceiptSurvivesFenceFailure(t *testing.T) {
	s := server{Options{Session: failingFence{}, Logger: slog.Default()}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/stock/BOOK/receipts", nil)
	s.finish(w, r, 200, ports.Outcome{Stock: domain.StockState{SKU: "BOOK", Version: 3}}, true, nil)
	if w.Code != 200 || w.Header().Get("Idempotent-Replay") != "true" || w.Header().Get("X-Session-Status") != "unavailable" || w.Header().Get("X-Session-Token") != "" || !strings.Contains(w.Body.String(), `"version":3`) {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
}
func TestSessionContractsRejectBeforeCommandPersistence(t *testing.T) {
	h := New(Options{Store: readStore{}, Session: failingFence{verify: readrouting.ErrToken}, TenantKeys: map[[32]byte]string{sha256.Sum256([]byte(testKey)): "acme"}})
	for _, tc := range []struct{ method, path, header, value string }{
		{"POST", "/v1/stock/BOOK/receipts", "X-Session-Token", "tampered"},
		{"GET", "/v1/stock/BOOK/projection", "X-Read-Consistency", "replica"},
		{"GET", "/v1/stock/BOOK", "X-Read-Wait-Ms", "2001"},
		{"POST", "/v1/stock/BOOK/receipts", "X-Read-Consistency", "replica"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"quantity":1}`))
		r.Header.Set("Authorization", "Bearer "+testKey)
		r.Header.Set(tc.header, tc.value)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "x")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal(tc, w.Code, w.Body.String())
		}
	}
}
