package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
)

var ErrInvalid = errors.New("invalid fulfillment input")
var ErrConflict = errors.New("shipment version or transition conflict")
var ErrNotFound = errors.New("shipment not found")
var idPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var slugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var skuPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

const (
	Ready       = "ready"
	Picking     = "picking"
	Packed      = "packed"
	Dispatching = "dispatching"
	Shipped     = "shipped"
	Delivered   = "delivered"
)

func ValidID(id string) bool { return idPattern.MatchString(id) }
func Warehouse(id string) string {
	if id == "" {
		return "default"
	}
	return id
}
func ShipmentID(checkout, warehouse string) string {
	sum := sha256.Sum256([]byte("atlas/shipment/" + checkout + "/" + Warehouse(warehouse)))
	sum[6] = (sum[6] & 15) | 0x50
	sum[8] = (sum[8] & 63) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

type Allocation struct {
	ID       string `json:"id"`
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}
type State struct {
	TenantID          string       `json:"tenant_id"`
	ID                string       `json:"id"`
	OrderID           string       `json:"order_id"`
	CheckoutID        string       `json:"checkout_id"`
	WarehouseID       string       `json:"warehouse_id"`
	Lines             []Allocation `json:"lines"`
	Status            string       `json:"status"`
	Version           int64        `json:"version"`
	Dispatched        int          `json:"dispatched"`
	TrackingReference string       `json:"tracking_reference,omitempty"`
}
type Shipment struct{ state State }

func Create(tenant, id, order, checkout, warehouse string, lines []Allocation) (*Shipment, error) {
	return Restore(State{TenantID: tenant, ID: id, OrderID: order, CheckoutID: checkout, WarehouseID: Warehouse(warehouse), Lines: lines, Status: Ready, Version: 1})
}
func Restore(s State) (*Shipment, error) {
	if !slugPattern.MatchString(s.TenantID) || !ValidID(s.ID) || !ValidID(s.OrderID) || !ValidID(s.CheckoutID) || !slugPattern.MatchString(s.WarehouseID) || s.Version < 1 || len(s.Lines) < 1 || len(s.Lines) > 100 || s.Dispatched < 0 || s.Dispatched > len(s.Lines) {
		return nil, ErrInvalid
	}
	seen := map[string]bool{}
	skus := map[string]bool{}
	for _, a := range s.Lines {
		if !ValidID(a.ID) || !skuPattern.MatchString(a.SKU) || a.Quantity < 1 || a.Quantity > 1_000_000_000 || seen[a.ID] || skus[a.SKU] {
			return nil, ErrInvalid
		}
		seen[a.ID] = true
		skus[a.SKU] = true
	}
	switch s.Status {
	case Ready, Picking, Packed:
		if s.Dispatched != 0 || s.TrackingReference != "" {
			return nil, ErrInvalid
		}
	case Dispatching:
		if s.Dispatched == len(s.Lines) || !validTracking(s.TrackingReference) {
			return nil, ErrInvalid
		}
	case Shipped, Delivered:
		if s.Dispatched != len(s.Lines) || !validTracking(s.TrackingReference) {
			return nil, ErrInvalid
		}
	default:
		return nil, ErrInvalid
	}
	s.Lines = append([]Allocation(nil), s.Lines...)
	return &Shipment{state: s}, nil
}
func validTracking(s string) bool {
	if len(s) < 1 || len(s) > 128 || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
func (s *Shipment) Snapshot() State {
	out := s.state
	out.Lines = append([]Allocation(nil), out.Lines...)
	return out
}
func (s *Shipment) Change(action, tracking string) error {
	if s.state.Version == math.MaxInt64 {
		return ErrConflict
	}
	var target, from string
	switch action {
	case "pick":
		from, target = Ready, Picking
	case "pack":
		from, target = Picking, Packed
	case "dispatch":
		from, target = Packed, Dispatching
		if !validTracking(tracking) {
			return ErrInvalid
		}
	case "deliver":
		from, target = Shipped, Delivered
	default:
		return ErrInvalid
	}
	if action != "dispatch" && tracking != "" {
		return ErrInvalid
	}
	if s.state.Status != from {
		return ErrConflict
	}
	s.state.Status = target
	s.state.Version++
	if action == "dispatch" {
		s.state.TrackingReference = tracking
	}
	return nil
}
func (s *Shipment) MarkDispatched() error {
	if s.state.Status != Dispatching || s.state.Version == math.MaxInt64 {
		return ErrConflict
	}
	s.state.Dispatched++
	s.state.Version++
	if s.state.Dispatched == len(s.state.Lines) {
		s.state.Status = Shipped
	}
	return nil
}
