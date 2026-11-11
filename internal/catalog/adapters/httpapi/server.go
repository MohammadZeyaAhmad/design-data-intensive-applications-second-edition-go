package httpapi

import (
	"encoding/json"
	"errors"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/describe"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/publish"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/quote"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"net/http"
)

type catalogStore interface {
	ports.Store
	ports.Content
}

func New(store catalogStore, keys map[[32]byte]string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/catalog/brands/{id}", func(w http.ResponseWriter, r *http.Request) {
		b, e := store.Brand(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
		if e != nil {
			failure(w, e)
			return
		}
		httpx.JSON(w, 200, b)
	})
	mux.HandleFunc("PUT /v1/catalog/brands/{id}", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Name            string `json:"name"`
			ExpectedVersion int64  `json:"expected_version"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		out, replayed, e := (describe.Handler{Store: store}).Brand(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), domain.Brand{ID: r.PathValue("id"), Name: b.Name, Version: b.ExpectedVersion})
		if e != nil {
			failure(w, e)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("PUT /v1/catalog/products/{sku}/details", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			BrandID         string           `json:"brand_id"`
			Description     string           `json:"description"`
			Attributes      json.RawMessage  `json:"attributes"`
			Variants        []domain.Variant `json:"variants"`
			Categories      []string         `json:"categories"`
			ExpectedVersion int64            `json:"expected_version"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		if len(b.Attributes) == 0 {
			b.Attributes = json.RawMessage(`{}`)
		}
		out, replayed, e := (describe.Handler{Store: store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), domain.Details{SKU: r.PathValue("sku"), BrandID: b.BrandID, Description: b.Description, Attributes: b.Attributes, Variants: b.Variants, Categories: b.Categories, Version: b.ExpectedVersion})
		if e != nil {
			failure(w, e)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("GET /v1/catalog/products/{sku}/details", func(w http.ResponseWriter, r *http.Request) {
		d, e := store.Details(r.Context(), auth.Tenant(r.Context()), r.PathValue("sku"))
		if e != nil {
			failure(w, e)
			return
		}
		httpx.JSON(w, 200, d)
	})
	mux.HandleFunc("PUT /v1/catalog/products/{sku}", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Name            string `json:"name"`
			UnitPriceMinor  int64  `json:"unit_price_minor"`
			Currency        string `json:"currency"`
			ExpectedVersion int64  `json:"expected_version"`
		}
		if !httpx.Decode(w, r, &b) {
			return
		}
		out, replayed, err := (publish.Handler{Store: store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), domain.Product{SKU: r.PathValue("sku"), Name: b.Name, UnitPriceMinor: b.UnitPriceMinor, Currency: b.Currency, Version: b.ExpectedVersion})
		if err != nil {
			failure(w, err)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("GET /v1/catalog/products/{sku}", func(w http.ResponseWriter, r *http.Request) {
		out, err := store.Product(r.Context(), auth.Tenant(r.Context()), r.PathValue("sku"))
		if err != nil {
			failure(w, err)
			return
		}
		httpx.JSON(w, 200, out)
	})
	mux.HandleFunc("POST /v1/catalog/quotes", func(w http.ResponseWriter, r *http.Request) {
		var c quote.Command
		if !httpx.Decode(w, r, &c) {
			return
		}
		out, replayed, err := (quote.Handler{Store: store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), c)
		if err != nil {
			failure(w, err)
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replay", "true")
		}
		httpx.JSON(w, 201, out)
	})
	mux.HandleFunc("GET /v1/catalog/quotes/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !identity.ValidID(r.PathValue("id")) {
			failure(w, api.ErrInvalid)
			return
		}
		out, err := store.Quote(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
		if err != nil {
			failure(w, err)
			return
		}
		httpx.JSON(w, 200, out)
	})
	return httpx.Guard(auth.Middleware(keys)(mux), nil)
}
func failure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalid) || errors.Is(err, api.ErrInvalid) || errors.Is(err, commands.ErrInvalid):
		httpx.Problem(w, 400, "invalid_input")
	case errors.Is(err, api.ErrNotFound):
		httpx.Problem(w, 404, "not_found")
	case errors.Is(err, api.ErrConflict) || errors.Is(err, commands.ErrConflict):
		httpx.Problem(w, 409, "version_or_request_conflict")
	default:
		httpx.Problem(w, 500, "catalog_failure")
	}
}
