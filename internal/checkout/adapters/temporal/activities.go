package temporal

import (
	"context"
	"errors"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/advance"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	temporalapi "go.temporal.io/sdk/temporal"
)

type Activities struct {
	Store   ports.TemporalStore
	Handler advance.Handler
}

func (a Activities) Advance(ctx context.Context, in ports.TemporalInput) (ports.TemporalResult, error) {
	j, cached, err := a.Store.TemporalClaim(ctx, in)
	if errors.Is(err, ports.ErrOwner) || errors.Is(err, ports.ErrStep) {
		return ports.TemporalResult{}, temporalapi.NewNonRetryableApplicationError("checkout owner or step mismatch", "execution_fence", err)
	}
	if err != nil {
		return ports.TemporalResult{}, err
	}
	if cached != nil {
		return *cached, nil
	}
	ctx, err = envelope.Worker(ctx, j.Metadata, "checkout", j.Request.ID)
	if err != nil {
		return ports.TemporalResult{}, err
	}
	next, stepErr := a.Handler.Handle(ctx, j)
	// A retry keeps the same ordinal. Successful progress and its return value
	// commit together, so a lost activity completion never advances twice.
	return a.Store.TemporalSave(ctx, j, in.Step, next, stepErr != nil)
}
