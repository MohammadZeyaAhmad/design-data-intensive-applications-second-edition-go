package postgres

import (
	"context"
	"errors"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	Pool     *pgxpool.Pool
	Provider ports.Provider
}

var _ api.Service = (*Service)(nil)

func (s *Service) prepare(ctx context.Context, i api.Intent) (api.Receipt, error) {
	if !domain.Valid(i.TenantID, i.ID, i.AmountMinor, i.Currency, i.Method) {
		return api.Receipt{}, domain.ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return api.Receipt{}, err
	}
	defer cmdtx.Rollback(tx)
	if _, err = tx.Exec(ctx, `INSERT INTO payments.intents(tenant_id,id,amount_minor,currency,method) VALUES($1,$2::uuid,$3,$4,$5) ON CONFLICT DO NOTHING`, i.TenantID, i.ID, i.AmountMinor, i.Currency, i.Method); err != nil {
		return api.Receipt{}, err
	}
	var r api.Receipt
	var method string
	err = tx.QueryRow(ctx, `SELECT id::text,status,amount_minor,currency,method FROM payments.intents WHERE tenant_id=$1 AND id=$2::uuid`, i.TenantID, i.ID).Scan(&r.ID, &r.Status, &r.AmountMinor, &r.Currency, &method)
	if err != nil {
		return r, err
	}
	if r.AmountMinor != i.AmountMinor || r.Currency != i.Currency || method != i.Method {
		return r, api.ErrConflict
	}
	return r, tx.Commit(ctx)
}
func (s *Service) Get(ctx context.Context, tenant, id string) (api.Receipt, error) {
	var r api.Receipt
	err := s.Pool.QueryRow(ctx, `SELECT id::text,status,amount_minor,currency FROM payments.intents WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id).Scan(&r.ID, &r.Status, &r.AmountMinor, &r.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		err = api.ErrNotFound
	}
	return r, err
}
func (s *Service) Capture(ctx context.Context, i api.Intent) (api.Receipt, error) {
	r, err := s.prepare(ctx, i)
	if err != nil {
		return r, err
	}
	if r.Status != "pending" {
		return r, nil
	}
	r, err = s.Provider.Get(ctx, i)
	if err != nil || r.Status == "pending" {
		r, err = s.Provider.Capture(ctx, i)
	}
	if err != nil {
		return api.Receipt{}, api.ErrUnknown
	}
	return s.apply(ctx, i, r)
}
func (s *Service) Close(ctx context.Context, i api.Intent) (api.Receipt, error) {
	r, err := s.prepare(ctx, i)
	if err != nil {
		return r, err
	}
	if domain.Closed(r.Status) {
		return r, nil
	}
	r, err = s.Provider.Close(ctx, i)
	if err != nil {
		return api.Receipt{}, api.ErrUnknown
	}
	return s.apply(ctx, i, r)
}
func (s *Service) apply(ctx context.Context, i api.Intent, r api.Receipt) (api.Receipt, error) {
	if r.ID != i.ID || r.AmountMinor != i.AmountMinor || r.Currency != i.Currency {
		return api.Receipt{}, api.ErrConflict
	}
	if r.Status != "captured" && !domain.Closed(r.Status) {
		return api.Receipt{}, api.ErrUnknown
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return api.Receipt{}, err
	}
	defer cmdtx.Rollback(tx)
	var current string
	if err = tx.QueryRow(ctx, `SELECT status FROM payments.intents WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, i.TenantID, i.ID).Scan(&current); err != nil {
		return api.Receipt{}, err
	}
	if domain.Closed(current) {
		if current == "refunded" || current == r.Status {
			return api.Receipt{ID: i.ID, Status: current, AmountMinor: i.AmountMinor, Currency: i.Currency}, tx.Commit(ctx)
		}
		return api.Receipt{}, api.ErrConflict
	}
	if r.Status == "captured" || r.Status == "refunded" {
		if err = post(ctx, tx, i, "capture", "provider_clearing", "customer_liability"); err != nil {
			return api.Receipt{}, err
		}
	}
	if r.Status == "refunded" {
		if err = post(ctx, tx, i, "refund", "customer_liability", "provider_clearing"); err != nil {
			return api.Receipt{}, err
		}
	}
	if current == "captured" && (r.Status == "declined" || r.Status == "voided") {
		return api.Receipt{}, api.ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE payments.intents SET status=$3 WHERE tenant_id=$1 AND id=$2::uuid`, i.TenantID, i.ID, r.Status); err != nil {
		return api.Receipt{}, err
	}
	return r, tx.Commit(ctx)
}
func post(ctx context.Context, tx pgx.Tx, i api.Intent, kind, debit, credit string) error {
	_, err := tx.Exec(ctx, `INSERT INTO payments.journal(tenant_id,payment_id,kind,currency,amount_minor,debit_account,credit_account) VALUES($1,$2::uuid,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, i.TenantID, i.ID, kind, i.Currency, i.AmountMinor, debit, credit)
	return err
}
