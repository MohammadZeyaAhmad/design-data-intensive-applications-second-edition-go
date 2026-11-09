package httpapi

import (
	"errors"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/auth"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
)

type Entry struct {
	Kind        string `json:"kind"`
	Currency    string `json:"currency"`
	AmountMinor int64  `json:"amount_minor"`
	Debit       string `json:"debit_account"`
	Credit      string `json:"credit_account"`
}

func New(store *postgres.Service, pool *pgxpool.Pool, keys map[[32]byte]string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/payments/{id}", func(w http.ResponseWriter, r *http.Request) {
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
	mux.HandleFunc("GET /v1/payments/{id}/journal", func(w http.ResponseWriter, r *http.Request) {
		if !identity.ValidID(r.PathValue("id")) {
			httpx.Problem(w, 400, "invalid_input")
			return
		}
		if _, err := store.Get(r.Context(), auth.Tenant(r.Context()), r.PathValue("id")); err != nil {
			failure(w, err)
			return
		}
		rows, err := pool.Query(r.Context(), `SELECT kind,currency,amount_minor,debit_account,credit_account FROM payments.journal WHERE tenant_id=$1 AND payment_id=$2::uuid ORDER BY recorded_at,id`, auth.Tenant(r.Context()), r.PathValue("id"))
		if err != nil {
			httpx.Problem(w, 500, "journal_failure")
			return
		}
		defer rows.Close()
		entries := []Entry{}
		for rows.Next() {
			var e Entry
			if err = rows.Scan(&e.Kind, &e.Currency, &e.AmountMinor, &e.Debit, &e.Credit); err != nil {
				httpx.Problem(w, 500, "journal_failure")
				return
			}
			entries = append(entries, e)
		}
		if rows.Err() != nil {
			httpx.Problem(w, 500, "journal_failure")
			return
		}
		httpx.JSON(w, 200, map[string]any{"entries": entries})
	})
	return httpx.Guard(auth.Middleware(keys)(mux), nil)
}

func failure(w http.ResponseWriter, err error) {
	if errors.Is(err, api.ErrNotFound) {
		httpx.Problem(w, 404, "not_found")
	} else {
		httpx.Problem(w, 503, "payments_unavailable")
	}
}
