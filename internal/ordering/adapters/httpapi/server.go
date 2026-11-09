package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/amendorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/cancelorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/placeorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
)

type Options struct {
	Store      ports.Store
	TenantKeys map[[32]byte]string
	Logger     *slog.Logger
}
type server struct{ Options }

func New(o Options) http.Handler {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	s := server{o}
	mux := http.NewServeMux()
	authenticate := auth.Middleware(o.TenantKeys)
	mux.HandleFunc("POST /v1/orders", s.place)
	mux.HandleFunc("POST /v1/orders/{id}/amend", s.amend)
	mux.HandleFunc("POST /v1/orders/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /v1/orders/{id}", s.load)
	mux.HandleFunc("GET /v1/orders/{id}/projection", s.projection)
	mux.HandleFunc("GET /v1/orders/{id}/events", s.events)
	return httpx.Guard(authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Match") != "" {
			httpx.Problem(w, 400, "use_expected_version")
			return
		}
		if id := r.PathValue("id"); id != "" && !domain.ValidID(id) {
			httpx.Problem(w, 400, "invalid_input")
			return
		}
		mux.ServeHTTP(w, r)
	})), o.Logger)
}
func (s server) place(w http.ResponseWriter, r *http.Request) {
	var c placeorder.Command
	if !httpx.Decode(w, r, &c) {
		return
	}
	state, replayed, err := (placeorder.Handler{Store: s.Store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), c)
	if err == nil {
		w.Header().Set("Location", "/v1/orders/"+state.ID)
	}
	s.result(w, 201, state, replayed, err)
}
func (s server) amend(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExpectedVersion int64         `json:"expected_version"`
		Lines           []domain.Line `json:"lines"`
	}
	if !httpx.Decode(w, r, &body) {
		return
	}
	state, replayed, err := (amendorder.Handler{Store: s.Store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), amendorder.Command{ID: r.PathValue("id"), ExpectedVersion: body.ExpectedVersion, Lines: body.Lines})
	s.result(w, 200, state, replayed, err)
}
func (s server) cancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ExpectedVersion int64  `json:"expected_version"`
		Reason          string `json:"reason"`
	}
	if !httpx.Decode(w, r, &body) {
		return
	}
	state, replayed, err := (cancelorder.Handler{Store: s.Store}).Handle(r.Context(), auth.Tenant(r.Context()), r.Header.Get("Idempotency-Key"), cancelorder.Command{ID: r.PathValue("id"), ExpectedVersion: body.ExpectedVersion, Reason: body.Reason})
	s.result(w, 200, state, replayed, err)
}
func (s server) load(w http.ResponseWriter, r *http.Request) {
	if !validID(w, r) {
		return
	}
	state, err := s.Store.Load(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
	if err != nil {
		s.failure(w, err)
		return
	}
	w.Header().Set("X-Consistency", "authoritative")
	w.Header().Set("ETag", `"`+strconv.FormatInt(state.Version, 10)+`"`)
	httpx.JSON(w, 200, state)
}
func (s server) projection(w http.ResponseWriter, r *http.Request) {
	if !validID(w, r) {
		return
	}
	minimum, ok := number(w, r, "min_version", 0, 0, 1<<63-1)
	if !ok {
		return
	}
	state, err := s.Store.Projection(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"))
	if err != nil {
		s.failure(w, err)
		return
	}
	if state.Version < minimum {
		w.Header().Set("Retry-After", "1")
		httpx.Problem(w, 409, "projection_not_ready")
		return
	}
	w.Header().Set("X-Consistency", "eventual")
	httpx.JSON(w, 200, state)
}
func (s server) events(w http.ResponseWriter, r *http.Request) {
	if !validID(w, r) {
		return
	}
	after, ok := number(w, r, "after_version", 0, 0, 1<<63-1)
	if !ok {
		return
	}
	limit, ok := number(w, r, "limit", 50, 1, 100)
	if !ok {
		return
	}
	page, err := s.Store.Events(r.Context(), auth.Tenant(r.Context()), r.PathValue("id"), after, int(limit))
	if err != nil {
		s.failure(w, err)
		return
	}
	httpx.JSON(w, 200, page)
}
func validID(w http.ResponseWriter, r *http.Request) bool {
	if !domain.ValidID(r.PathValue("id")) {
		httpx.Problem(w, 400, "invalid_input")
		return false
	}
	return true
}
func number(w http.ResponseWriter, r *http.Request, name string, fallback, minimum, maximum int64) (int64, bool) {
	text := r.URL.Query().Get(name)
	if text == "" {
		return fallback, true
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n < minimum || n > maximum {
		httpx.Problem(w, 400, "invalid_input")
		return 0, false
	}
	return n, true
}
func (s server) result(w http.ResponseWriter, status int, state domain.State, replayed bool, err error) {
	if err != nil {
		s.failure(w, err)
		return
	}
	w.Header().Set("Idempotent-Replay", strconv.FormatBool(replayed))
	w.Header().Set("ETag", `"`+strconv.FormatInt(state.Version, 10)+`"`)
	httpx.JSON(w, status, state)
}
func (s server) failure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidInput):
		httpx.Problem(w, 400, "invalid_input")
	case errors.Is(err, domain.ErrNotFound):
		httpx.Problem(w, 404, "not_found")
	case errors.Is(err, domain.ErrConflict):
		httpx.Problem(w, 409, "stream_version_conflict")
	case errors.Is(err, domain.ErrIdempotencyConflict):
		httpx.Problem(w, 409, "idempotency_conflict")
	case errors.Is(err, domain.ErrInvalidState):
		httpx.Problem(w, 409, "invalid_transition")
	case errors.Is(err, domain.ErrUnsupportedSchema):
		httpx.Problem(w, 503, "unsupported_event_schema")
	case errors.Is(err, context.DeadlineExceeded):
		httpx.Problem(w, 504, "deadline_exceeded")
	case errors.Is(err, context.Canceled):
		httpx.Problem(w, 408, "request_canceled")
	default:
		s.Logger.Error("ordering operation failed", "error", err)
		httpx.Problem(w, 500, "internal_error")
	}
}
