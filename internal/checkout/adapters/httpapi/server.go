package httpapi

import (
	"errors"
	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/start"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"io"
	"net/http"
)

func New(h start.Handler, store ports.Store, keys map[[32]byte]string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/orders/{id}/checkout", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			ExpectedVersion int64  `json:"expected_version"`
			QuoteID         string `json:"quote_id"`
			PaymentMethod   string `json:"payment_method"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		if !identity.ValidID(r.PathValue("id")) || !identity.ValidID(b.QuoteID) || !start.ValidMethod(b.PaymentMethod) {
			httpx.Problem(w, 400, "invalid_input")
			return
		}
		out, replayed, err := h.Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), start.Command{OrderID: r.PathValue("id"), ExpectedVersion: b.ExpectedVersion, QuoteID: b.QuoteID, PaymentMethod: b.PaymentMethod})
		if err != nil {
			failure(w, err)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 202, out)
	})
	mux.HandleFunc("GET /v1/checkouts/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !identity.ValidID(r.PathValue("id")) {
			httpx.Problem(w, 400, "invalid_input")
			return
		}
		out, err := store.Get(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
		if err != nil {
			failure(w, err)
			return
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("POST /v1/checkouts/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		if !identity.ValidID(r.PathValue("id")) {
			httpx.Problem(w, 400, "invalid_input")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1)
		b, err := io.ReadAll(r.Body)
		if err != nil || len(b) != 0 {
			httpx.Problem(w, 400, "unexpected_body")
			return
		}
		if err = store.Cancel(r.Context(), auth.Tenant(r.Context()), r.PathValue("id")); err != nil {
			failure(w, err)
			return
		}
		httpx.JSON(w, 202, map[string]bool{"cancel_requested": true})
	})
	return httpx.Guard(auth.Middleware(keys)(mux), nil)
}
func failure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, orders.ErrInvalid) || errors.Is(err, cat.ErrInvalid) || errors.Is(err, cat.ErrExpired):
		httpx.Problem(w, 400, "invalid_quote_or_input")
	case errors.Is(err, orders.ErrNotFound) || errors.Is(err, cat.ErrNotFound) || errors.Is(err, postgres.ErrNotFound):
		httpx.Problem(w, 404, "not_found")
	case errors.Is(err, orders.ErrConflict) || errors.Is(err, postgres.ErrTerminal):
		httpx.Problem(w, 409, "checkout_conflict")
	default:
		httpx.Problem(w, 503, "checkout_unavailable")
	}
}
