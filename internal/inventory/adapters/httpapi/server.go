package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/changereservation"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/receivestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/reservestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type Options struct {
	Store      ports.Store
	TenantKeys map[[32]byte]string
	Ready      func(context.Context) error
	Logger     *slog.Logger
}
type server struct{ Options }
type tenantKey struct{}

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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		defer func() {
			if recover() != nil {
				s.Logger.Error("request panic")
				problem(w, 500, "internal_error")
			}
		}()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || len(key) > 256 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			problem(w, 401, "unauthorized")
			return
		}
		tenant, ok := s.TenantKeys[sha256.Sum256([]byte(key))]
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			problem(w, 401, "unauthorized")
			return
		}
		// Tenant identity is bound to the credential, never selected by a body
		// field or X-Tenant-ID header supplied by the caller.
		if r.Header.Get("If-Match") != "" {
			problem(w, http.StatusBadRequest, "conditional_requests_not_supported")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tenant)))
	})
}
func tenant(r *http.Request) string { return r.Context().Value(tenantKey{}).(string) }

func (s server) receive(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Quantity int64 `json:"quantity"`
	}
	if !decode(w, r, &input) {
		return
	}
	out, replayed, err := (receivestock.Handler{Store: s.Store}).Handle(r.Context(), tenant(r), r.Header.Get("Idempotency-Key"), receivestock.Command{SKU: r.PathValue("sku"), Quantity: input.Quantity})
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
	out, replayed, err := (changereservation.Handler{Store: s.Store}).Handle(r.Context(), tenant(r), r.Header.Get("Idempotency-Key"), changereservation.Command{ID: r.PathValue("id"), Target: target})
	s.result(w, 200, out, replayed, err)
}
func (s server) stock(w http.ResponseWriter, r *http.Request) {
	if !domain.ValidSKU(r.PathValue("sku")) {
		problem(w, 400, "invalid_input")
		return
	}
	out, err := s.Store.Stock(r.Context(), tenant(r), r.PathValue("sku"))
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
	out, err := s.Store.Projection(r.Context(), tenant(r), r.PathValue("sku"))
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
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		problem(w, 415, "unsupported_media_type")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err = d.Decode(target); err != nil {
		problem(w, 400, "invalid_json")
		return false
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		problem(w, 400, "invalid_json")
		return false
	}
	return true
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, status int, code string) {
	write(w, status, map[string]any{"error": map[string]string{"code": code}})
}
