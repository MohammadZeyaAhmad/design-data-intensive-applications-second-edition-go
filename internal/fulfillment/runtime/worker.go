package runtime

import (
	"context"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/features/dispatch"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
)

type Worker struct {
	Store   ports.Store
	Orders  api.Dispatcher
	Handler dispatch.Handler
}

func (w Worker) ProcessOne(ctx context.Context) (bool, error) {
	j, found, err := w.Store.Claim(ctx)
	if err != nil || !found {
		return found, err
	}
	ctx, err = envelope.Worker(ctx, j.Metadata, "fulfillment", j.State.ID)
	if err != nil {
		return true, err
	}
	err = w.Handler.Handle(ctx, j)
	if saveErr := w.Store.Save(ctx, j, err != nil); saveErr != nil {
		return true, saveErr
	}
	return true, err
}
func (w Worker) Run(ctx context.Context, onError func(error)) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		var sent bool
		var err error
		if w.Orders != nil {
			sent, err = w.Orders.DispatchFulfillmentOne(step, w.Store)
		}
		if err != nil && ctx.Err() == nil {
			onError(err)
		}
		worked, workErr := w.ProcessOne(step)
		cancel()
		if workErr != nil && ctx.Err() == nil {
			onError(workErr)
		}
		if !sent && !worked || err != nil || workErr != nil {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
