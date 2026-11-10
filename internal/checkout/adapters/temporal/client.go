package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type Starter struct {
	Client client.Client
	Queue  string
}

func WorkflowID(e ports.Execution) string { return "atlas-checkout/" + e.TenantID + "/" + e.ID }
func (s Starter) Start(ctx context.Context, e ports.Execution) error {
	if !e.Valid() {
		return ports.ErrStep
	}
	queue := s.Queue
	if queue == "" {
		queue = TaskQueue
	}
	_, err := s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: WorkflowID(e), TaskQueue: queue,
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, WorkflowName, ports.TemporalInput{Execution: e})
	var duplicate *serviceerror.WorkflowExecutionAlreadyStarted
	if !errors.As(err, &duplicate) {
		return err
	}
	// Never acknowledge a colliding ID belonging to another workflow type.
	d, err := s.Client.DescribeWorkflowExecution(ctx, WorkflowID(e), "")
	if err != nil {
		return err
	}
	if d.WorkflowExecutionInfo.GetType().GetName() != WorkflowName {
		return fmt.Errorf("Temporal workflow ID collision")
	}
	return nil
}
func (s Starter) Wake(ctx context.Context, e ports.Execution) error {
	if !e.Valid() {
		return ports.ErrStep
	}
	err := s.Client.SignalWorkflow(ctx, WorkflowID(e), "", RefreshSignal, "control")
	var absent *serviceerror.NotFound
	if errors.As(err, &absent) {
		// A terminal execution can close between the PostgreSQL check and RPC.
		d, describeErr := s.Client.DescribeWorkflowExecution(ctx, WorkflowID(e), "")
		if describeErr == nil && d.WorkflowExecutionInfo.GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil
		}
	}
	return err
}

func Register(w worker.Worker, a Activities) {
	w.RegisterWorkflowWithOptions(Checkout, workflow.RegisterOptions{Name: WorkflowName})
	w.RegisterActivityWithOptions(a.Advance, activity.RegisterOptions{Name: ActivityName})
}
