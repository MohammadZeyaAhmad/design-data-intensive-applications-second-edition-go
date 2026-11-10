package advance

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	inv "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/inventory"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	pay "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
)

type Handler struct {
	Inventory   inv.Service
	Payments    pay.Service
	Orders      orders.Service
	HoldSeconds int64
}

func StepID(checkout, sku string) string {
	sum := sha256.Sum256([]byte("atlas/inventory/" + checkout + "/" + sku))
	sum[6] = (sum[6] & 15) | 0x50
	sum[8] = (sum[8] & 63) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}
func (h Handler) Handle(ctx context.Context, j ports.Job) (domain.State, error) {
	r, s := j.Request, j.State
	if j.CancelRequested && s.Stage != domain.Compensating {
		s = domain.State{Stage: domain.Compensating, Reason: "cancelled"}
	}
	intent := pay.Intent{TenantID: r.TenantID, ID: r.ID, AmountMinor: r.Quote.TotalMinor, Currency: r.Quote.Currency, Method: r.PaymentMethod}
	fail := func(reason string) domain.State { return domain.State{Stage: domain.Compensating, Reason: reason} }
	switch s.Stage {
	case domain.Reserving:
		if s.Index >= len(r.Quote.Lines) {
			return domain.State{Stage: domain.Capturing}, nil
		}
		line := r.Quote.Lines[s.Index]
		ttl := h.HoldSeconds
		if ttl == 0 {
			ttl = 120
		}
		_, err := h.Inventory.Reserve(ctx, r.TenantID, StepID(r.ID, line.SKU), inv.Line{SKU: line.SKU, Quantity: line.Quantity}, ttl)
		if errors.Is(err, inv.ErrUnavailable) || errors.Is(err, inv.ErrClosed) {
			return fail("inventory_unavailable"), nil
		}
		if err != nil {
			return s, err
		}
		s.Index++
		return s, nil
	case domain.Capturing:
		receipt, err := h.Payments.Capture(ctx, intent)
		if err != nil {
			return s, err
		}
		if receipt.Status != "captured" {
			return fail("payment_declined"), nil
		}
		return domain.State{Stage: domain.Allocating}, nil
	case domain.Allocating:
		if s.Index >= len(r.Quote.Lines) {
			return domain.State{Stage: domain.Confirming}, nil
		}
		line := r.Quote.Lines[s.Index]
		hold, err := h.Inventory.Commit(ctx, r.TenantID, StepID(r.ID, line.SKU))
		if errors.Is(err, inv.ErrUnavailable) || errors.Is(err, inv.ErrClosed) {
			return fail("reservation_expired"), nil
		}
		if err != nil {
			return s, err
		}
		if hold.Status != "committed" {
			return fail("reservation_expired"), nil
		}
		s.Index++
		return s, nil
	case domain.Confirming:
		receipt, err := h.Payments.Get(ctx, r.TenantID, r.ID)
		if err != nil {
			return s, err
		}
		if receipt.Status != "captured" {
			return fail("payment_declined"), nil
		}
		proof := orders.Proof{CheckoutID: r.ID, PaymentID: receipt.ID, AmountMinor: receipt.AmountMinor, Currency: receipt.Currency}
		for _, line := range r.Quote.Lines {
			hold, err := h.Inventory.Get(ctx, r.TenantID, StepID(r.ID, line.SKU))
			if errors.Is(err, inv.ErrUnavailable) || errors.Is(err, inv.ErrClosed) {
				return fail("reservation_expired"), nil
			}
			if err != nil {
				return s, err
			}
			if hold.Status != "committed" {
				return fail("reservation_expired"), nil
			}
			proof.Allocations = append(proof.Allocations, orders.Allocation{ID: hold.ID, SKU: hold.SKU, WarehouseID: hold.WarehouseID, Quantity: hold.Quantity})
		}
		out, err := h.Orders.Confirm(ctx, r.TenantID, r.OrderID, proof)
		if err != nil {
			return s, err
		}
		if out.Status != "confirmed" {
			return fail(out.FailureReason), nil
		}
		return domain.State{Stage: domain.Confirmed}, nil
	case domain.Compensating:
		// Fence confirmation in the owning aggregate BEFORE compensating external
		// resources. A stale worker can no longer confirm a failed order.
		out, err := h.Orders.Fail(ctx, r.TenantID, r.OrderID, r.ID, s.Reason)
		if err != nil {
			return s, err
		}
		if out.Status == "confirmed" {
			return domain.State{Stage: domain.Confirmed}, nil
		}
		receipt, err := h.Payments.Close(ctx, intent)
		if err != nil {
			return s, err
		}
		if receipt.Status == "captured" || receipt.Status == "pending" {
			return s, pay.ErrUnknown
		}
		if s.Index < len(r.Quote.Lines) {
			line := r.Quote.Lines[s.Index]
			if err = h.Inventory.Close(ctx, r.TenantID, StepID(r.ID, line.SKU), inv.Line{SKU: line.SKU, Quantity: line.Quantity}); err != nil {
				return s, err
			}
			s.Index++
			return s, nil
		}
		if s.Reason == "cancelled" {
			s.Stage = domain.Cancelled
		} else {
			s.Stage = domain.Failed
		}
		return s, nil
	default:
		return s, errors.New("unknown checkout phase")
	}
}
