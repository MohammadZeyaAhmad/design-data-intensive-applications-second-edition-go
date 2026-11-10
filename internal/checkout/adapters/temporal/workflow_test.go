package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	temporalapi "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

var execution = ports.Execution{TenantID: "acme", ID: "79480f5e-76b9-4dbb-ad25-4239cb8e14b3"}

func TestReplayRecordedV0History(t *testing.T) {
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflowWithOptions(Checkout, workflow.RegisterOptions{Name: WorkflowName})
	if err := r.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/checkout-v0.json"); err != nil {
		t.Fatal(err)
	}
}

func TestRefreshWakesDurableWaitWithoutAdvancingStep(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var calls []ports.TemporalInput
	env.RegisterActivityWithOptions(func(_ context.Context, in ports.TemporalInput) (ports.TemporalResult, error) {
		calls = append(calls, in)
		if len(calls) == 1 {
			return ports.TemporalResult{State: domain.State{Stage: domain.Capturing}, Wait: "paused"}, nil
		}
		return ports.TemporalResult{State: domain.State{Stage: domain.Confirmed}}, nil
	}, activity.RegisterOptions{Name: ActivityName})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(RefreshSignal, "control") }, 100*time.Millisecond)
	env.ExecuteWorkflow(Checkout, ports.TemporalInput{Execution: execution, Step: 7})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].Step != 7 || calls[1].Step != 7 {
		t.Fatal("wake advanced a logical step", calls)
	}
}

func TestNativeCancellationRunsDisconnectedCompensation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var forced bool
	env.RegisterActivityWithOptions(func(_ context.Context, in ports.TemporalInput) (ports.TemporalResult, error) {
		if in.ForceCancel {
			forced = true
			return ports.TemporalResult{State: domain.State{Stage: domain.Cancelled}}, nil
		}
		return ports.TemporalResult{State: domain.State{Stage: domain.Capturing}, Wait: "retry"}, nil
	}, activity.RegisterOptions{Name: ActivityName})
	env.RegisterDelayedCallback(env.CancelWorkflow, 100*time.Millisecond)
	env.ExecuteWorkflow(Checkout, ports.TemporalInput{Execution: execution})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var out domain.State
	if err := env.GetWorkflowResult(&out); err != nil || !forced || out.Stage != domain.Cancelled {
		t.Fatal("compensation missing", out, err)
	}
}

func TestContinueAsNewPreservesGlobalOrdinalAndCancellation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(_ context.Context, _ ports.TemporalInput) (ports.TemporalResult, error) {
		return ports.TemporalResult{State: domain.State{Stage: domain.Compensating}}, nil
	}, activity.RegisterOptions{Name: ActivityName})
	env.ExecuteWorkflow(Checkout, ports.TemporalInput{Execution: execution, Step: 9, ForceCancel: true})
	var continued *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &continued) {
		t.Fatal("history was not bounded", env.GetWorkflowError())
	}
	var next ports.TemporalInput
	if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); err != nil {
		t.Fatal(err)
	}
	if next.Step != 9+MaxTurns || !next.ForceCancel || next.Execution != execution {
		t.Fatal("continuation lost state", next)
	}
}

func TestRepeatedActivityCancellationStillBoundsHistory(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(context.Context, ports.TemporalInput) (ports.TemporalResult, error) {
		return ports.TemporalResult{}, temporalapi.NewCanceledError("activity worker stopped")
	}, activity.RegisterOptions{Name: ActivityName})
	env.ExecuteWorkflow(Checkout, ports.TemporalInput{Execution: execution, Step: 9})
	var continued *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &continued) {
		t.Fatal("cancelled attempts escaped the history bound", env.GetWorkflowError())
	}
	var next ports.TemporalInput
	if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); err != nil {
		t.Fatal(err)
	}
	if next.Step != 9 || next.ForceCancel {
		t.Fatal("activity cancellation changed business intent", next)
	}
}
