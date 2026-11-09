package domain

const (
	ReservationActive    = "active"
	ReservationCommitted = "committed"
	ReservationReleased  = "released"
)

type ReservationState struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
	Status   string `json:"status"`
	Version  int64  `json:"version"`
}

type Reservation struct{ state ReservationState }

func NewReservation(id, tenant, sku string, quantity int64) (*Reservation, error) {
	return RestoreReservation(ReservationState{ID: id, TenantID: tenant, SKU: sku, Quantity: quantity, Status: ReservationActive, Version: 1})
}

func RestoreReservation(s ReservationState) (*Reservation, error) {
	if s.ID == "" || !ValidTenant(s.TenantID) || !ValidSKU(s.SKU) || !ValidQuantity(s.Quantity) || s.Version < 1 {
		return nil, ErrCorruptState
	}
	switch s.Status {
	case ReservationActive, ReservationCommitted, ReservationReleased:
	default:
		return nil, ErrCorruptState
	}
	return &Reservation{state: s}, nil
}

func (r *Reservation) Snapshot() ReservationState { return r.state }

// Transition returns false for a repeated terminal action. Compensation of an
// allocation is a different future business command, never an ordinary release.
func (r *Reservation) Transition(target string) (bool, error) {
	if target != ReservationCommitted && target != ReservationReleased {
		return false, ErrInvalidInput
	}
	if r.state.Status == target {
		return false, nil
	}
	if r.state.Status != ReservationActive {
		return false, ErrInvalidTransition
	}
	r.state.Status = target
	r.state.Version++
	return true, nil
}
