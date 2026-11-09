package provider

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	Pool  *pgxpool.Pool
	Token string
}

func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments/{id}/capture", s.operation)
	mux.HandleFunc("POST /v1/payments/{id}/close", s.operation)
	mux.HandleFunc("POST /v1/payments/{id}/lookup", s.operation)
	expected := sha256.Sum256([]byte(s.Token))
	return httpx.Guard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			if err := s.Pool.Ping(r.Context()); err != nil {
				httpx.Problem(w, 503, "not_ready")
				return
			}
			httpx.JSON(w, 200, map[string]string{"status": "ready"})
			return
		}
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		actual := sha256.Sum256([]byte(key))
		if !ok || len(s.Token) < 32 || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
			httpx.Problem(w, 401, "unauthorized")
			return
		}
		mux.ServeHTTP(w, r)
	}), nil)
}
func (s Server) operation(w http.ResponseWriter, r *http.Request) {
	var intent api.Intent
	if !httpx.Decode(w, r, &intent) {
		return
	}
	if intent.ID != r.PathValue("id") || !domain.Valid(intent.TenantID, intent.ID, intent.AmountMinor, intent.Currency, intent.Method) {
		httpx.Problem(w, 400, "invalid_input")
		return
	}
	kind := "capture"
	if strings.HasSuffix(r.URL.Path, "/close") {
		kind = "close"
	}
	if strings.HasSuffix(r.URL.Path, "/lookup") {
		kind = "lookup"
	}
	// Deliberately model work continuing when a client loses its connection.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if kind == "capture" && intent.Method == "test-slow" {
		time.Sleep(2 * time.Second)
	}
	receipt, drop, err := s.Operate(ctx, intent, kind)
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.Problem(w, 404, "not_found")
		return
	}
	if errors.Is(err, api.ErrConflict) {
		httpx.Problem(w, 409, "identity_conflict")
		return
	}
	if err != nil {
		httpx.Problem(w, 500, "provider_failure")
		return
	}
	if drop {
		if hijack, ok := w.(http.Hijacker); ok {
			conn, _, err := hijack.Hijack()
			if err == nil {
				_ = conn.Close()
				return
			}
		}
		httpx.Problem(w, 503, "response_lost")
		return
	}
	httpx.JSON(w, 200, receipt)
}

// Capture and close serialize on one durable intent. Closing an unseen intent
// creates a tombstone: a delayed capture cannot create a new charge afterward.
func (s Server) Operate(ctx context.Context, i api.Intent, kind string) (api.Receipt, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return api.Receipt{}, false, err
	}
	defer cmdtx.Rollback(tx)
	if kind != "lookup" {
		if _, err = tx.Exec(ctx, `INSERT INTO provider.operations(tenant_id,id,amount_minor,currency,method) VALUES($1,$2::uuid,$3,$4,$5) ON CONFLICT DO NOTHING`, i.TenantID, i.ID, i.AmountMinor, i.Currency, i.Method); err != nil {
			return api.Receipt{}, false, err
		}
	}
	var out api.Receipt
	var method string
	var lostCapture, lostRefund bool
	err = tx.QueryRow(ctx, `SELECT id::text,status,amount_minor,currency,method,capture_response_lost,refund_response_lost FROM provider.operations WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, i.TenantID, i.ID).Scan(&out.ID, &out.Status, &out.AmountMinor, &out.Currency, &method, &lostCapture, &lostRefund)
	if err != nil {
		return out, false, err
	}
	if out.AmountMinor != i.AmountMinor || out.Currency != i.Currency || method != i.Method {
		return out, false, api.ErrConflict
	}
	drop := false
	if kind == "capture" && out.Status == "pending" {
		if method == "test-decline" {
			out.Status = "declined"
		} else {
			out.Status = "captured"
		}
		if method == "test-lost-response" && !lostCapture {
			lostCapture = true
			drop = true
		}
	}
	if kind == "close" {
		switch out.Status {
		case "pending":
			out.Status = "voided"
		case "captured":
			out.Status = "refunded"
		}
		if method == "test-refund-lost-response" && !lostRefund {
			lostRefund = true
			drop = true
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE provider.operations SET status=$3,capture_response_lost=$4,refund_response_lost=$5 WHERE tenant_id=$1 AND id=$2::uuid`, i.TenantID, i.ID, out.Status, lostCapture, lostRefund); err != nil {
		return out, false, err
	}
	return out, drop, tx.Commit(ctx)
}
