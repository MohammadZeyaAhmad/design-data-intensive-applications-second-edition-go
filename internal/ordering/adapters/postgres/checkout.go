package postgres

import (
	"context"
	"encoding/json"
	"errors"
	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5"
	"time"
)

type Checkout struct{ Store *Store }

var _ api.Service = Checkout{}

func quoteView(s domain.State) cat.Quote {
	if s.Pricing == nil {
		return cat.Quote{}
	}
	p := s.Pricing
	q := cat.Quote{ID: p.QuoteID, TenantID: s.TenantID, Currency: p.Currency, TotalMinor: p.TotalMinor, ExpiresAt: p.ExpiresAt}
	for _, l := range p.Lines {
		q.Lines = append(q.Lines, cat.PriceLine{SKU: l.SKU, Quantity: l.Quantity, UnitPriceMinor: l.UnitPriceMinor, PriceVersion: l.PriceVersion})
	}
	return q
}
func view(s domain.State) api.View {
	return api.View{ID: s.CheckoutID, OrderID: s.ID, Status: s.Status, Version: s.Version, Quote: quoteView(s), FailureReason: s.FailureReason}
}
func checkoutError(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return api.ErrNotFound
	case errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrInvalidState) || errors.Is(err, domain.ErrIdempotencyConflict):
		return api.ErrConflict
	case errors.Is(err, domain.ErrInvalidInput):
		return api.ErrInvalid
	default:
		return err
	}
}
func (c Checkout) Begin(ctx context.Context, tenant, order, key string, expected int64, q cat.Quote, method string) (api.View, bool, error) {
	if expected < 1 || q.TenantID != tenant || len(method) < 1 || len(method) > 128 {
		return api.View{}, false, api.ErrInvalid
	}
	payload := struct {
		OrderID         string `json:"order_id"`
		ExpectedVersion int64  `json:"expected_version"`
		QuoteID         string `json:"quote_id"`
		Method          string `json:"payment_method"`
	}{order, expected, q.ID, method}
	r, err := ports.NewRequest(tenant, "begin-checkout", key, payload)
	if err != nil {
		return api.View{}, false, checkoutError(err)
	}
	r.CheckoutMethod = method
	id, err := identity.NewID()
	if err != nil {
		return api.View{}, false, err
	}
	p := domain.Pricing{QuoteID: q.ID, Currency: q.Currency, TotalMinor: q.TotalMinor, ExpiresAt: q.ExpiresAt}
	for _, l := range q.Lines {
		p.Lines = append(p.Lines, domain.PriceLine{SKU: l.SKU, Quantity: l.Quantity, UnitPriceMinor: l.UnitPriceMinor, PriceVersion: l.PriceVersion})
	}
	s, replayed, err := c.Store.ExecuteAt(ctx, r, order, expected, func(o *domain.Order, now time.Time) ([]domain.Event, error) { return o.BeginCheckout(id, p, now) })
	return view(s), replayed, checkoutError(err)
}
func (c Checkout) Confirm(ctx context.Context, tenant, order string, proof api.Proof) (api.View, error) {
	for range 3 {
		s, err := c.Store.Load(ctx, tenant, order)
		if err != nil {
			return api.View{}, checkoutError(err)
		}
		if s.CheckoutID != proof.CheckoutID {
			return api.View{}, api.ErrConflict
		}
		if s.Status == domain.Confirmed || s.Status == domain.CheckoutFailedState {
			return view(s), nil
		}
		p := domain.PaymentProof{CheckoutID: proof.CheckoutID, PaymentID: proof.PaymentID, Currency: proof.Currency, AmountMinor: proof.AmountMinor}
		for _, a := range proof.Allocations {
			p.Allocations = append(p.Allocations, domain.Allocation{ID: a.ID, SKU: a.SKU, WarehouseID: a.WarehouseID, Quantity: a.Quantity})
		}
		r, err := ports.NewRequest(tenant, "confirm-checkout", "confirm-"+proof.CheckoutID, p)
		if err != nil {
			return api.View{}, checkoutError(err)
		}
		out, _, err := c.Store.Execute(ctx, r, order, s.Version, func(o *domain.Order) ([]domain.Event, error) { return o.ConfirmCheckout(p) })
		if errors.Is(err, domain.ErrConflict) {
			continue
		}
		return view(out), checkoutError(err)
	}
	return api.View{}, api.ErrConflict
}
func (c Checkout) Fail(ctx context.Context, tenant, order, id, reason string) (api.View, error) {
	for range 3 {
		s, err := c.Store.Load(ctx, tenant, order)
		if err != nil {
			return api.View{}, checkoutError(err)
		}
		if s.CheckoutID != id {
			return api.View{}, api.ErrConflict
		}
		if s.Status == domain.Confirmed || s.Status == domain.CheckoutFailedState {
			return view(s), nil
		}
		payload := struct{ ID, Reason string }{id, reason}
		r, err := ports.NewRequest(tenant, "fail-checkout", "fail-"+id, payload)
		if err != nil {
			return api.View{}, checkoutError(err)
		}
		out, _, err := c.Store.Execute(ctx, r, order, s.Version, func(o *domain.Order) ([]domain.Event, error) { return o.FailCheckout(id, reason) })
		if errors.Is(err, domain.ErrConflict) {
			continue
		}
		return view(out), checkoutError(err)
	}
	return api.View{}, api.ErrConflict
}
func enqueueCheckout(ctx context.Context, tx pgx.Tx, tenant, method string, state domain.State) error {
	if len(method) < 1 || len(method) > 128 {
		return domain.ErrInvalidInput
	}
	request := api.Requested{ID: state.CheckoutID, TenantID: tenant, OrderID: state.ID, Quote: quoteView(state), PaymentMethod: method}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ordering.checkout_outbox(tenant_id,id,order_id,body) VALUES($1,$2::uuid,$3::uuid,$4)`, tenant, request.ID, state.ID, body)
	return err
}
func (s *Store) DispatchOne(ctx context.Context, sink api.Sink) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	var body []byte
	var tenant, id string
	err = tx.QueryRow(ctx, `SELECT tenant_id,id::text,body FROM ordering.checkout_outbox WHERE dispatched_at IS NULL ORDER BY tenant_id,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&tenant, &id, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var request api.Requested
	if err = json.Unmarshal(body, &request); err != nil {
		return false, err
	}
	if request.TenantID != tenant || request.ID != id {
		return false, domain.ErrInvalidEvent
	}
	if err = sink.Submit(ctx, request); err != nil {
		return true, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ordering.checkout_outbox SET dispatched_at=now() WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
