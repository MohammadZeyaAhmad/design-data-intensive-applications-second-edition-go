package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"regexp"
	"strings"
)

var (
	ErrInvalidInput        = errors.New("invalid order input")
	ErrInvalidState        = errors.New("order transition is not allowed")
	ErrConflict            = errors.New("order stream version conflict")
	ErrNotFound            = errors.New("order not found")
	ErrIdempotencyConflict = errors.New("order request key has different payload")
	ErrInvalidEvent        = errors.New("invalid order event or sequence")
	ErrUnsupportedSchema   = errors.New("unsupported order event schema")
)

const (
	Placed         = "placed"
	Cancelled      = "cancelled"
	OrderPlaced    = "order.placed"
	LinesReplaced  = "order.lines-replaced"
	OrderCancelled = "order.cancelled"
)

var tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var skuPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var idPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func ValidTenant(s string) bool { return tenantPattern.MatchString(s) }
func ValidID(s string) bool     { return idPattern.MatchString(s) }

type Line struct {
	SKU      string `json:"sku"`
	Quantity int64  `json:"quantity"`
}
type State struct {
	ID                 string `json:"id"`
	TenantID           string `json:"tenant_id"`
	Version            int64  `json:"version"`
	Status             string `json:"status"`
	Lines              []Line `json:"lines"`
	CancellationReason string `json:"cancellation_reason,omitempty"`
}
type Event struct {
	Version       int64           `json:"version"`
	Type          string          `json:"type"`
	SchemaVersion int             `json:"schema_version"`
	Data          json.RawMessage `json:"data"`
}
type Order struct{ state State }

func New(tenant, id string) (*Order, error) {
	if !ValidTenant(tenant) || !ValidID(id) {
		return nil, ErrInvalidInput
	}
	return &Order{state: State{ID: strings.ToLower(id), TenantID: tenant}}, nil
}
func ValidLines(lines []Line) bool {
	if len(lines) == 0 || len(lines) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, line := range lines {
		if !skuPattern.MatchString(line.SKU) || line.Quantity < 1 || line.Quantity > 1_000_000_000 || seen[line.SKU] {
			return false
		}
		seen[line.SKU] = true
	}
	return true
}
func validReason(s string) bool {
	return s == "customer_requested" || s == "inventory_unavailable" || s == "payment_failed"
}
func Restore(s State) (*Order, error) {
	if !ValidTenant(s.TenantID) || !ValidID(s.ID) || s.Version < 1 || !ValidLines(s.Lines) {
		return nil, ErrInvalidEvent
	}
	if s.Status != Placed && s.Status != Cancelled {
		return nil, ErrInvalidEvent
	}
	if (s.Status == Cancelled && !validReason(s.CancellationReason)) || (s.Status == Placed && s.CancellationReason != "") {
		return nil, ErrInvalidEvent
	}
	s.ID = strings.ToLower(s.ID)
	s.Lines = append([]Line(nil), s.Lines...)
	return &Order{state: s}, nil
}
func (o *Order) Snapshot() State { s := o.state; s.Lines = append([]Line(nil), s.Lines...); return s }

func (o *Order) event(kind string, schema int, data any) ([]Event, error) {
	if o.state.Version == math.MaxInt64 {
		return nil, ErrInvalidState
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return []Event{{Version: o.state.Version + 1, Type: kind, SchemaVersion: schema, Data: b}}, nil
}
func (o *Order) Place(lines []Line) ([]Event, error) {
	if o.state.Version != 0 {
		return nil, ErrInvalidState
	}
	if !ValidLines(lines) {
		return nil, ErrInvalidInput
	}
	return o.event(OrderPlaced, 2, struct {
		Lines []Line `json:"lines"`
	}{lines})
}
func (o *Order) ReplaceLines(lines []Line) ([]Event, error) {
	if o.state.Status != Placed {
		return nil, ErrInvalidState
	}
	if !ValidLines(lines) {
		return nil, ErrInvalidInput
	}
	return o.event(LinesReplaced, 1, struct {
		Lines []Line `json:"lines"`
	}{lines})
}
func (o *Order) Cancel(reason string) ([]Event, error) {
	if !validReason(reason) {
		return nil, ErrInvalidInput
	}
	if o.state.Status != Placed {
		return nil, ErrInvalidState
	}
	return o.event(OrderCancelled, 1, struct {
		Reason string `json:"reason"`
	}{reason})
}

func decode(b []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return ErrInvalidEvent
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrInvalidEvent
	}
	return nil
}

// Apply is the same deterministic reducer used by command rehydration, replay,
// snapshots and projections. Schema 1 placement events are upcast at this edge.
func (o *Order) Apply(e Event) error {
	if o.state.Version == math.MaxInt64 || e.Version != o.state.Version+1 {
		return ErrInvalidEvent
	}
	next := o.Snapshot()
	switch e.Type {
	case OrderPlaced:
		if next.Version != 0 {
			return ErrInvalidEvent
		}
		var lines []Line
		switch e.SchemaVersion {
		case 1:
			var old struct {
				Items []struct {
					SKU string `json:"sku"`
					Qty int64  `json:"qty"`
				} `json:"items"`
			}
			if err := decode(e.Data, &old); err != nil {
				return err
			}
			for _, item := range old.Items {
				lines = append(lines, Line{SKU: item.SKU, Quantity: item.Qty})
			}
		case 2:
			var current struct {
				Lines []Line `json:"lines"`
			}
			if err := decode(e.Data, &current); err != nil {
				return err
			}
			lines = current.Lines
		default:
			return ErrUnsupportedSchema
		}
		if !ValidLines(lines) {
			return ErrInvalidEvent
		}
		next.Lines = lines
		next.Status = Placed
	case LinesReplaced:
		if e.SchemaVersion != 1 {
			return ErrUnsupportedSchema
		}
		if next.Status != Placed {
			return ErrInvalidEvent
		}
		var payload struct {
			Lines []Line `json:"lines"`
		}
		if err := decode(e.Data, &payload); err != nil {
			return err
		}
		if !ValidLines(payload.Lines) {
			return ErrInvalidEvent
		}
		next.Lines = payload.Lines
	case OrderCancelled:
		if e.SchemaVersion != 1 {
			return ErrUnsupportedSchema
		}
		if next.Status != Placed {
			return ErrInvalidEvent
		}
		var payload struct {
			Reason string `json:"reason"`
		}
		if err := decode(e.Data, &payload); err != nil {
			return err
		}
		if !validReason(payload.Reason) {
			return ErrInvalidEvent
		}
		next.Status = Cancelled
		next.CancellationReason = payload.Reason
	default:
		return ErrUnsupportedSchema
	}
	next.Version = e.Version
	o.state = next
	return nil
}
