package ports

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
)

var ErrOwner = errors.New("checkout orchestration owner mismatch")
var ErrStep = errors.New("Temporal logical step disagrees with durable receipt")

type Execution struct {
	TenantID string `json:"tenant_id"`
	ID       string `json:"id"`
}

var executionTenant = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func (e Execution) Valid() bool {
	return executionTenant.MatchString(e.TenantID) && envelope.ValidID(e.ID) && e.ID == strings.ToLower(e.ID)
}

type TemporalInput struct {
	Execution
	Step        int64 `json:"step"`
	ForceCancel bool  `json:"force_cancel,omitempty"`
}
type TemporalResult struct {
	State domain.State `json:"state"`
	Wait  string       `json:"wait,omitempty"`
}
type TemporalStarter interface {
	Start(context.Context, Execution) error
	Wake(context.Context, Execution) error
}
type TemporalStore interface {
	TemporalClaim(context.Context, TemporalInput) (Job, *TemporalResult, error)
	TemporalSave(context.Context, Job, int64, domain.State, bool) (TemporalResult, error)
}
