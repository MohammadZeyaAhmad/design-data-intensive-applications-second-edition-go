// Package temporal schedules the same checkout feature used by the PostgreSQL
// process manager. Workflow code never calls a database, provider or wall clock.
package temporal

import (
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	temporalapi "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName  = "atlas.checkout.v1"
	ActivityName  = "atlas.checkout.advance.v1"
	TaskQueue     = "atlas-checkout-v1"
	RefreshSignal = "refresh"
	LoopChange    = "checkout-loop"
	MaxTurns      = 64
)

// Checkout keeps only identifiers and scheduling state in Temporal history.
// Requests, prices, payment method references and receipts stay in PostgreSQL.
func Checkout(ctx workflow.Context, in ports.TemporalInput) (domain.State, error) {
	if !in.Execution.Valid() || in.Step < 0 {
		return domain.State{}, temporalapi.NewNonRetryableApplicationError("invalid execution", "invalid_execution", nil)
	}
	version := workflow.GetVersion(ctx, LoopChange, workflow.DefaultVersion, 1)
	delay := time.Second // v0 histories contain no version marker and use 1s.
	if version >= 1 {
		delay = 500 * time.Millisecond
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		WaitForCancellation: true,
		RetryPolicy:         &temporalapi.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 5 * time.Second},
	})
	for turn := 0; ; turn++ {
		if ctx.Err() != nil {
			// Native cancellation still executes the business compensation fence.
			ctx, _ = workflow.NewDisconnectedContext(ctx)
			in.ForceCancel = true
		}
		if turn >= MaxTurns {
			// Include cancelled activity attempts in the history bound. The
			// global step and sticky cancellation survive across runs.
			return domain.State{}, workflow.NewContinueAsNewError(ctx, WorkflowName, in)
		}
		var out ports.TemporalResult
		err := workflow.ExecuteActivity(ctx, ActivityName, in).Get(ctx, &out)
		if temporalapi.IsCanceledError(err) {
			continue
		}
		if err != nil {
			return domain.State{}, err
		}
		if domain.Terminal(out.State.Stage) {
			return out.State, nil
		}
		if out.Wait == "" {
			in.Step++
		} else {
			// Every wait is durable. Controls send a coalescible hint; activities
			// always re-read the authoritative flags before taking a lease.
			waitCtx, cancel := workflow.WithCancel(ctx)
			timer := workflow.NewTimer(waitCtx, delay)
			selector := workflow.NewSelector(ctx)
			selector.AddFuture(timer, func(workflow.Future) {})
			selector.AddReceive(workflow.GetSignalChannel(ctx, RefreshSignal), func(c workflow.ReceiveChannel, _ bool) {
				var hint string
				c.Receive(ctx, &hint)
			})
			selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
			selector.Select(ctx)
			cancel()
		}
	}
}
