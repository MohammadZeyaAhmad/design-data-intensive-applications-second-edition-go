// Package operations describes local operator controls, separately from public
// tenant APIs. Controls change scheduling, never business state or effect IDs.
package operations

import (
	"context"
	"errors"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
)

var ErrInvalid = errors.New("invalid workflow control")
var ErrNotFound = errors.New("workflow not found")
var ErrConflict = errors.New("workflow control version or state conflict")
var ErrBusy = errors.New("workflow has an active lease")
var ErrTerminal = errors.New("workflow is terminal")
var ErrForbidden = errors.New("operator scope required")

type Command struct {
	TenantID        string `json:"tenant_id"`
	ID              string `json:"id"`
	Action          string `json:"action"`
	Reason          string `json:"reason"`
	ExpectedVersion int64  `json:"expected_control_version"`
	Key             string `json:"-"`
}
type View struct {
	TenantID       string            `json:"tenant_id"`
	ID             string            `json:"id"`
	Kind           string            `json:"kind"`
	Status         string            `json:"status"`
	Paused         bool              `json:"paused"`
	ControlVersion int64             `json:"control_version"`
	LeaseToken     int64             `json:"lease_token"`
	LeaseActive    bool              `json:"lease_active"`
	Attempts       int64             `json:"attempts"`
	LastError      string            `json:"last_error,omitempty"`
	Metadata       envelope.Metadata `json:"metadata"`
}
type Result struct {
	View    View   `json:"view"`
	AuditID string `json:"audit_id"`
}
type Audit struct {
	ID         string            `json:"audit_id"`
	Version    int64             `json:"version"`
	Action     string            `json:"action"`
	Reason     string            `json:"reason"`
	Before     View              `json:"before"`
	After      View              `json:"after"`
	Metadata   envelope.Metadata `json:"metadata"`
	RecordedAt time.Time         `json:"recorded_at"`
}
type Page struct {
	Entries          []Audit `json:"entries"`
	NextAfterVersion int64   `json:"next_after_version"`
}
type Workflow interface {
	Inspect(context.Context, string, string) (View, error)
	Control(context.Context, Command) (Result, bool, error)
	ControlHistory(context.Context, string, string, int64, int) (Page, error)
}
