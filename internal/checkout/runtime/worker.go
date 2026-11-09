package runtime

import (
	"context"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/advance"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"time"
)

type Worker struct {
	Store   ports.Store
	Orders  orders.Dispatcher
	Handler advance.Handler
}

func (w Worker) ProcessOne(ctx context.Context) (bool, error) {
	j, found, err := w.Store.Claim(ctx)
	if err != nil || !found {
		return found, err
	}
	next, err := w.Handler.Handle(ctx, j)
	retry := err != nil
	if retry {
		next = j.State
	}
	if saveErr := w.Store.Save(ctx, j, next, retry); saveErr != nil {
		return true, saveErr
	}
	return true, err
}
func (w Worker) Run(ctx context.Context, onError func(error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		sent, err := w.Orders.DispatchOne(step, w.Store)
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
