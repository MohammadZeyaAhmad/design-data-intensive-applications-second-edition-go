package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/changereservation"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/receivestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/reservestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type Options struct {
	Store      ports.Store
	TenantKeys map[[32]byte]string
	Ready      func(context.Context) error
	Logger     *slog.Logger
}
type server struct{ Options }

func New(o Options) http.Handler {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	s := server{o}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Ready(ctx); err != nil {
			problem(w, 503, "not_ready")
			return
		}
		write(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("POST /v1/stock/{sku}/receipts", s.auth(http.HandlerFunc(s.receive)))
	mux.Handle("GET /v1/stock/{sku}", s.auth(http.HandlerFunc(s.stock)))
	mux.Handle("GET /v1/stock/{sku}/projection", s.auth(http.HandlerFunc(s.projection)))
	mux.Handle("POST /v1/reservations", s.auth(http.HandlerFunc(s.reserve)))
	mux.Handle("GET /v1/reservations/{id}", s.auth(http.HandlerFunc(s.reservation)))
	mux.Handle("POST /v1/reservations/{id}/commit", s.auth(http.HandlerFunc(s.change)))
	mux.Handle("POST /v1/reservations/{id}/release", s.auth(http.HandlerFunc(s.change)))
	mux.Handle("POST /v1/reservations/{id}/reverse", s.auth(http.HandlerFunc(s.change)))
	return httpx.Guard(mux, o.Logger)
}

func (s server) auth(next http.Handler) http.Handler {
	return auth.Middleware(s.TenantKeys)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Match") != "" {
			problem(w, 400, "conditional_requests_not_supported")
			return
		}
		next.ServeHTTP(w, r)
	}))
}
func tenant(r *http.Request) string { return auth.Tenant(r.Context()) }

func (s server) receive(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Quantity    int64  `json:"quantity"`
		WarehouseID string `json:"warehouse_id,omitempty"`
	}
	if !decode(w, r, &input) {
		return
	}
	out, replayed, err := (receivestock.Handler{Store: s.Store}).Handle(r.Context(), tenant(r), r.Header.Get("Idempotency-Key"), receivestock.Command{SKU: r.PathValue("sku"), Quantity: input.Quantity, WarehouseID: input.WarehouseID})
	s.result(w, 200, out, replayed, err)
}
func (s server) reserve(w http.ResponseWriter, r *http.Request) {
	var c reservestock.Command
	if !decode(w, r, &c) {
		return
	}
	out, replayed, err := (reservestock.Handler{Store: s.Store}).Handle(r.Context(), tenant(r), r.Header.Get("Idempotency-Key"), c)
	if err == nil {
		w.Header().Set("Location", "/v1/reservations/"+out.Reservation.ID)
	}
	s.result(w, 201, out, replayed, err)
}
func (s server) change(w http.ResponseWriter, r *http.Request) {
	// Transition commands have no body. Reject unexpected input rather than
	// accepting an apparently conditional operation that ignores its condition.
	r.Body = http.MaxBytesReader(w, r.Body, 1)
	b, err := io.ReadAll(r.Body)
	if err != nil || len(b) > 0 {
		problem(w, 400, "unexpected_body")
		return
	}
	target := domain.ReservationReleased
	if strings.HasSuffix(r.URL.Path, "/commit") {
		target = domain.ReservationCommitted
	}
	if strings.HasSuffix(r.URL.Path, "/reverse") {
		target = domain.ReservationReversed
	}
	out, replayed, err := (changereservation.Handler{Store: s.Store}).Handle(r.Context(), tenant(r), r.Header.Get("Idempotency-Key"), changereservation.Command{ID: r.PathValue("id"), Target: target})
	s.result(w, 200, out, replayed, err)
}
func (s server) stock(w http.ResponseWriter, r *http.Request) {
	if !domain.ValidSKU(r.PathValue("sku")) {
		problem(w, 400, "invalid_input")
		return
	}
	warehouse := r.URL.Query().Get("warehouse_id")
	if !domain.ValidWarehouse(warehouse) {
		problem(w, 400, "invalid_input")
		return
	}
	var out domain.StockState
	var err error
	if warehouse == "" || warehouse == "default" {
		out, err = s.Store.Stock(r.Context(), tenant(r), r.PathValue("sku"))
	} else {
		out, err = s.Store.StockAt(r.Context(), tenant(r), r.PathValue("sku"), warehouse)
	}
	if err != nil {
		s.failure(w, err)
		return
	}
	w.Header().Set("ETag", `"`+strconv.FormatInt(out.Version, 10)+`"`)
	write(w, 200, out)
}
func (s server) projection(w http.ResponseWriter, r *http.Request) {
	if !domain.ValidSKU(r.PathValue("sku")) {
		problem(w, 400, "invalid_input")
		return
	}
	warehouse := r.URL.Query().Get("warehouse_id")
	if !domain.ValidWarehouse(warehouse) {
		problem(w, 400, "invalid_input")
		return
	}
	out, err := s.Store.ProjectionAt(r.Context(), tenant(r), r.PathValue("sku"), warehouse)
	if err != nil {
		s.failure(w, err)
		return
	}
	w.Header().Set("X-Consistency", "eventual")
	write(w, 200, out)
}
func (s server) reservation(w http.ResponseWriter, r *http.Request) {
	if !identity.ValidID(r.PathValue("id")) {
		problem(w, 400, "invalid_input")
		return
	}
	out, err := s.Store.Reservation(r.Context(), tenant(r), r.PathValue("id"))
	if err != nil {
		s.failure(w, err)
		return
	}
	write(w, 200, out)
}
func (s server) result(w http.ResponseWriter, status int, out ports.Outcome, replayed bool, err error) {
	if err != nil {
		s.failure(w, err)
		return
	}
	w.Header().Set("Idempotent-Replay", strconv.FormatBool(replayed))
	w.Header().Set("ETag", `"`+strconv.FormatInt(out.Stock.Version, 10)+`"`)
	write(w, status, out)
}
func (s server) failure(w http.ResponseWriter, err error) {
	var sqlError interface{ SQLState() string }
	if errors.As(err, &sqlError) {
		if sqlError.SQLState() == "57014" {
			problem(w, 504, "database_budget_exceeded")
			return
		}
		if sqlError.SQLState() == "55P03" {
			problem(w, 503, "database_busy")
			return
		}
	}
	switch {
	case errors.Is(err, domain.ErrInvalidInput):
		problem(w, 400, "invalid_input")
	case errors.Is(err, domain.ErrNotFound):
		problem(w, 404, "not_found")
	case errors.Is(err, domain.ErrInsufficientStock):
		problem(w, 409, "insufficient_stock")
	case errors.Is(err, domain.ErrIdempotencyConflict):
		problem(w, 409, "idempotency_conflict")
	case errors.Is(err, domain.ErrInvalidTransition):
		problem(w, 409, "invalid_transition")
	case errors.Is(err, domain.ErrReservationExpired):
		problem(w, 409, "reservation_expired")
	case errors.Is(err, domain.ErrConflict):
		problem(w, 409, "concurrent_modification")
	case errors.Is(err, context.DeadlineExceeded):
		problem(w, 504, "deadline_exceeded")
	case errors.Is(err, context.Canceled):
		problem(w, 408, "request_canceled")
	default:
		s.Logger.Error("inventory operation failed", "error", err)
		problem(w, 500, "internal_error")
	}
}
func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	return httpx.Decode(w, r, target)
}
func write(w http.ResponseWriter, status int, value any)     { httpx.JSON(w, status, value) }
func problem(w http.ResponseWriter, status int, code string) { httpx.Problem(w, status, code) }
