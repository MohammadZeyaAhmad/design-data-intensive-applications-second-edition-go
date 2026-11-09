package domain

import (
	"errors"
	"math"
	"time"
)

var ErrReservationExpired = errors.New("reservation deadline has passed")
var ErrReservationNotDue = errors.New("reservation has not reached its deadline")

const (
	ReservationActive    = "active"
	ReservationCommitted = "committed"
	ReservationReleased  = "released"
	ReservationExpired   = "expired"
	ReservationReversed  = "reversed"
)

type ReservationState struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	SKU       string    `json:"sku"`
	Quantity  int64     `json:"quantity"`
	Status    string    `json:"status"`
	Version   int64     `json:"version"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

type Reservation struct{ state ReservationState }

func NewReservation(id, tenant, sku string, quantity int64, expiresAt time.Time) (*Reservation, error) {
	return RestoreReservation(ReservationState{ID: id, TenantID: tenant, SKU: sku, Quantity: quantity, Status: ReservationActive, Version: 1, ExpiresAt: expiresAt.UTC()})
}

func RestoreReservation(s ReservationState) (*Reservation, error) {
	if s.ID == "" || !ValidTenant(s.TenantID) || !ValidSKU(s.SKU) || !ValidQuantity(s.Quantity) || s.Version < 1 || s.ExpiresAt.IsZero() {
		return nil, ErrCorruptState
	}
	switch s.Status {
	case ReservationActive, ReservationCommitted, ReservationReleased, ReservationExpired, ReservationReversed:
	default:
		return nil, ErrCorruptState
	}
	return &Reservation{state: s}, nil
}

func (r *Reservation) Snapshot() ReservationState { return r.state }

// Transition evaluates a caller-supplied authoritative clock after locking.
// Repeated terminal actions have no new effect. Reversal is explicit compensation.
func (r *Reservation) Transition(target string, now time.Time) (bool, error) {
	if now.IsZero() || (target != ReservationCommitted && target != ReservationReleased && target != ReservationExpired && target != ReservationReversed) {
		return false, ErrInvalidInput
	}
	if r.state.Status == target {
		return false, nil
	}
	if r.state.Version == math.MaxInt64 {
		return false, ErrCorruptState
	}
	if target == ReservationReversed {
		if r.state.Status != ReservationCommitted {
			return false, ErrInvalidTransition
		}
	} else if r.state.Status != ReservationActive {
		return false, ErrInvalidTransition
	}
	if target == ReservationCommitted && !now.Before(r.state.ExpiresAt) {
		return false, ErrReservationExpired
	}
	if target == ReservationExpired && now.Before(r.state.ExpiresAt) {
		return false, ErrReservationNotDue
	}
	r.state.Status = target
	r.state.Version++
	return true, nil
}
