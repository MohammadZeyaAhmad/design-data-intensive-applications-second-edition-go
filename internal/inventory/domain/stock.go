package domain

import (
	"errors"
	"math"
	"regexp"
)

var (
	ErrInvalidInput        = errors.New("invalid inventory input")
	ErrInsufficientStock   = errors.New("insufficient available stock")
	ErrInvalidTransition   = errors.New("invalid reservation transition")
	ErrNotFound            = errors.New("resource not found")
	ErrConflict            = errors.New("concurrent inventory modification")
	ErrIdempotencyConflict = errors.New("idempotency key already used for a different request")
	ErrCorruptState        = errors.New("inventory invariant violated")
)

const MaxCommandQuantity int64 = 1_000_000_000

var skuPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func ValidSKU(s string) bool     { return skuPattern.MatchString(s) }
func ValidTenant(s string) bool  { return tenantPattern.MatchString(s) }
func ValidQuantity(q int64) bool { return q > 0 && q <= MaxCommandQuantity }

// StockState is a persistence/query snapshot, not a mutable aggregate.
type StockState struct {
	TenantID  string `json:"tenant_id"`
	SKU       string `json:"sku"`
	OnHand    int64  `json:"on_hand"`
	Reserved  int64  `json:"reserved"`
	Allocated int64  `json:"allocated"`
	Version   int64  `json:"version"`
}

func (s StockState) Available() int64 { return s.OnHand - s.Reserved - s.Allocated }

type Stock struct{ state StockState }

func RestoreStock(s StockState) (*Stock, error) {
	if !ValidTenant(s.TenantID) || !ValidSKU(s.SKU) || s.Version < 0 || s.OnHand < 0 || s.Reserved < 0 || s.Allocated < 0 || s.Allocated > s.OnHand || s.Reserved > s.OnHand-s.Allocated {
		return nil, ErrCorruptState
	}
	return &Stock{state: s}, nil
}

func (s *Stock) Snapshot() StockState { return s.state }

func (s *Stock) Receive(q int64) error {
	if !ValidQuantity(q) || s.state.OnHand > math.MaxInt64-q || s.state.Version == math.MaxInt64 {
		return ErrInvalidInput
	}
	s.state.OnHand += q
	s.state.Version++
	return nil
}

func (s *Stock) Reserve(q int64) error {
	if !ValidQuantity(q) || s.state.Version == math.MaxInt64 {
		return ErrInvalidInput
	}
	if q > s.state.Available() {
		return ErrInsufficientStock
	}
	s.state.Reserved += q
	s.state.Version++
	return nil
}

func (s *Stock) Release(q int64) error {
	if !ValidQuantity(q) || q > s.state.Reserved || s.state.Version == math.MaxInt64 {
		return ErrCorruptState
	}
	s.state.Reserved -= q
	s.state.Version++
	return nil
}

func (s *Stock) Commit(q int64) error {
	if !ValidQuantity(q) || q > s.state.Reserved || s.state.Version == math.MaxInt64 {
		return ErrCorruptState
	}
	s.state.Reserved -= q
	s.state.Allocated += q
	s.state.Version++
	return nil
}

// ReverseAllocation compensates an allocation that has not been shipped.
func (s *Stock) ReverseAllocation(q int64) error {
	if !ValidQuantity(q) || q > s.state.Allocated || s.state.Version == math.MaxInt64 {
		return ErrCorruptState
	}
	s.state.Allocated -= q
	s.state.Version++
	return nil
}
