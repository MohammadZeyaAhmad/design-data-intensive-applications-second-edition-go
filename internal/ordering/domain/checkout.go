package domain

import (
	"regexp"
	"time"
)

const (
	CheckoutPending     = "checkout_pending"
	Confirmed           = "confirmed"
	CheckoutFailedState = "checkout_failed"
	CheckoutStarted     = "order.checkout-started"
	OrderConfirmed      = "order.confirmed"
	CheckoutFailed      = "order.checkout-failed"
)

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

type PriceLine struct {
	SKU            string `json:"sku"`
	Quantity       int64  `json:"quantity"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
	PriceVersion   int64  `json:"price_version"`
}
type Pricing struct {
	QuoteID    string      `json:"quote_id"`
	Currency   string      `json:"currency"`
	TotalMinor int64       `json:"total_minor"`
	Lines      []PriceLine `json:"lines"`
	ExpiresAt  time.Time   `json:"expires_at"`
}
type Allocation struct {
	ID       string `json:"id"`
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}
type PaymentProof struct {
	CheckoutID  string       `json:"checkout_id"`
	PaymentID   string       `json:"payment_id"`
	Currency    string       `json:"currency"`
	AmountMinor int64        `json:"amount_minor"`
	Allocations []Allocation `json:"allocations"`
}

func validPricing(p *Pricing, lines []Line) bool {
	if p == nil || !ValidID(p.QuoteID) || !currencyPattern.MatchString(p.Currency) || p.ExpiresAt.IsZero() || len(p.Lines) != len(lines) || p.TotalMinor < 1 || p.TotalMinor > 9_000_000_000_000_000 {
		return false
	}
	want := map[string]int64{}
	for _, l := range lines {
		want[l.SKU] = l.Quantity
	}
	var total int64
	for _, l := range p.Lines {
		if want[l.SKU] != l.Quantity || l.Quantity < 1 || l.PriceVersion < 1 || l.UnitPriceMinor < 1 || l.UnitPriceMinor > 9_000_000_000_000_000/l.Quantity {
			return false
		}
		delete(want, l.SKU)
		amount := l.UnitPriceMinor * l.Quantity
		if total > 9_000_000_000_000_000-amount {
			return false
		}
		total += amount
	}
	return len(want) == 0 && total == p.TotalMinor
}
func validFailure(reason string) bool {
	return reason == "inventory_unavailable" || reason == "payment_declined" || reason == "reservation_expired" || reason == "cancelled"
}
func (o *Order) BeginCheckout(id string, p Pricing, now time.Time) ([]Event, error) {
	if o.state.Status != Placed {
		return nil, ErrInvalidState
	}
	if !ValidID(id) || !validPricing(&p, o.state.Lines) || !now.Before(p.ExpiresAt) {
		return nil, ErrInvalidInput
	}
	return o.event(CheckoutStarted, 1, struct {
		CheckoutID string  `json:"checkout_id"`
		Pricing    Pricing `json:"pricing"`
	}{id, p})
}
func validProof(state State, p PaymentProof) bool {
	if state.Pricing == nil || p.CheckoutID != state.CheckoutID || p.PaymentID != state.CheckoutID || p.AmountMinor != state.Pricing.TotalMinor || p.Currency != state.Pricing.Currency || len(p.Allocations) != len(state.Lines) {
		return false
	}
	want := map[string]int64{}
	for _, l := range state.Lines {
		want[l.SKU] = l.Quantity
	}
	seen := map[string]bool{}
	for _, a := range p.Allocations {
		if !ValidID(a.ID) || seen[a.ID] || want[a.SKU] != a.Quantity {
			return false
		}
		seen[a.ID] = true
		delete(want, a.SKU)
	}
	return len(want) == 0
}
func (o *Order) ConfirmCheckout(p PaymentProof) ([]Event, error) {
	if o.state.Status != CheckoutPending {
		return nil, ErrInvalidState
	}
	if !validProof(o.state, p) {
		return nil, ErrInvalidInput
	}
	return o.event(OrderConfirmed, 1, p)
}
func (o *Order) FailCheckout(id, reason string) ([]Event, error) {
	if o.state.Status != CheckoutPending {
		return nil, ErrInvalidState
	}
	if id != o.state.CheckoutID || !validFailure(reason) {
		return nil, ErrInvalidInput
	}
	return o.event(CheckoutFailed, 1, struct {
		CheckoutID string `json:"checkout_id"`
		Reason     string `json:"reason"`
	}{id, reason})
}
func applyCheckout(next State, e Event) (State, error) {
	if e.SchemaVersion != 1 {
		return next, ErrUnsupportedSchema
	}
	switch e.Type {
	case CheckoutStarted:
		if next.Status != Placed {
			return next, ErrInvalidEvent
		}
		var p struct {
			CheckoutID string  `json:"checkout_id"`
			Pricing    Pricing `json:"pricing"`
		}
		if err := decode(e.Data, &p); err != nil {
			return next, err
		}
		if !ValidID(p.CheckoutID) || !validPricing(&p.Pricing, next.Lines) {
			return next, ErrInvalidEvent
		}
		next.Status = CheckoutPending
		next.CheckoutID = p.CheckoutID
		next.Pricing = &p.Pricing
	case OrderConfirmed:
		if next.Status != CheckoutPending {
			return next, ErrInvalidEvent
		}
		var p PaymentProof
		if err := decode(e.Data, &p); err != nil {
			return next, err
		}
		if !validProof(next, p) {
			return next, ErrInvalidEvent
		}
		next.Status = Confirmed
		next.PaymentID = p.PaymentID
	case CheckoutFailed:
		if next.Status != CheckoutPending {
			return next, ErrInvalidEvent
		}
		var p struct {
			CheckoutID string `json:"checkout_id"`
			Reason     string `json:"reason"`
		}
		if err := decode(e.Data, &p); err != nil {
			return next, err
		}
		if p.CheckoutID != next.CheckoutID || !validFailure(p.Reason) {
			return next, ErrInvalidEvent
		}
		next.Status = CheckoutFailedState
		next.FailureReason = p.Reason
	}
	return next, nil
}
