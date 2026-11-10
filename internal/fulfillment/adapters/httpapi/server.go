package httpapi

import (
	"errors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/features/changeshipment"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"net/http"
	"strconv"
)

func New(store ports.Store, keys map[[32]byte]string) http.Handler {
	mux := http.NewServeMux()
	valid := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("If-Match") != "" {
			httpx.Problem(w, 400, "conditional_requests_not_supported")
			return false
		}
		if !domain.ValidID(r.PathValue("id")) {
			httpx.Problem(w, 400, "invalid_input")
			return false
		}
		return true
	}
	mux.HandleFunc("GET /v1/shipments/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !valid(w, r) {
			return
		}
		v, err := store.Get(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
		respond(w, v, err)
	})
	mux.HandleFunc("GET /v1/shipments/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		if !valid(w, r) {
			return
		}
		v, err := store.History(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
		respond(w, v, err)
	})
	mux.HandleFunc("GET /v1/orders/{id}/fulfillment", func(w http.ResponseWriter, r *http.Request) {
		if !valid(w, r) {
			return
		}
		v, err := store.ForOrder(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
		respond(w, v, err)
	})
	for _, action := range []string{"pick", "pack", "dispatch", "deliver"} {
		mux.HandleFunc("POST /v1/shipments/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
			if !valid(w, r) {
				return
			}
			var c changeshipment.Command
			if !httpx.Decode(w, r, &c) {
				return
			}
			v, replayed, err := (changeshipment.Handler{Store: store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), r.PathValue("id"), action, c)
			if err != nil {
				respond(w, nil, err)
				return
			}
			w.Header().Set("Idempotent-Replay", strconv.FormatBool(replayed))
			status := 200
			if action == "dispatch" {
				status = 202
			}
			httpx.JSON(w, status, v)
		})
	}
	return httpx.Guard(auth.Middleware(keys)(mux), nil)
}
func respond(w http.ResponseWriter, v any, err error) {
	if err == nil {
		httpx.JSON(w, 200, v)
		return
	}
	switch {
	case errors.Is(err, domain.ErrNotFound):
		httpx.Problem(w, 404, "not_found")
	case errors.Is(err, domain.ErrInvalid) || errors.Is(err, commands.ErrInvalid):
		httpx.Problem(w, 400, "invalid_input")
	case errors.Is(err, domain.ErrConflict) || errors.Is(err, commands.ErrConflict):
		httpx.Problem(w, 409, "shipment_conflict")
	default:
		httpx.Problem(w, 503, "temporarily_unavailable")
	}
}
