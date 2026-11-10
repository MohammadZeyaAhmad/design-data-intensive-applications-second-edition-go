//go:build integration && temporal

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	checktemporal "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/temporal"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
)

func temporalClient(t *testing.T) client.Client {
	t.Helper()
	address := os.Getenv("ATLAS_TEMPORAL_TEST_ADDRESS")
	if address != "127.0.0.1:17233" {
		t.Fatal("real Temporal tests require the dedicated local server at 127.0.0.1:17233")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, client.Options{HostPort: address})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func temporalWorker(t *testing.T, c client.Client, queue string, a checktemporal.Activities, legacy bool) worker.Worker {
	t.Helper()
	w := worker.New(c, queue, worker.Options{WorkerStopTimeout: 100 * time.Millisecond})
	if legacy {
		w.RegisterWorkflowWithOptions(legacyCheckout, workflow.RegisterOptions{Name: checktemporal.WorkflowName})
		w.RegisterActivityWithOptions(a.Advance, activity.RegisterOptions{Name: checktemporal.ActivityName})
	} else {
		checktemporal.Register(w, a)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	return w
}
func temporalRun(t *testing.T, c client.Client, e ports.Execution) domain.State {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var state domain.State
	if err := c.GetWorkflow(ctx, checktemporal.WorkflowID(e), "").Get(ctx, &state); err != nil {
		t.Fatal(err)
	}
	return state
}
func temporalHistory(t *testing.T, c client.Client, e ports.Execution) *historypb.History {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	it := c.GetWorkflowHistory(ctx, checktemporal.WorkflowID(e), "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	h := &historypb.History{}
	for it.HasNext() {
		event, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		h.Events = append(h.Events, event)
	}
	return h
}

// legacyCheckout is the pre-version-marker implementation. It really runs on
// the server, then its history is replayed through the current implementation.
func legacyCheckout(ctx workflow.Context, in ports.TemporalInput) (domain.State, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, WaitForCancellation: true, RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 5 * time.Second}})
	for turn := 0; ; turn++ {
		if ctx.Err() != nil {
			ctx, _ = workflow.NewDisconnectedContext(ctx)
			in.ForceCancel = true
		}
		if turn >= checktemporal.MaxTurns {
			return domain.State{}, workflow.NewContinueAsNewError(ctx, checktemporal.WorkflowName, in)
		}
		var out ports.TemporalResult
		err := workflow.ExecuteActivity(ctx, checktemporal.ActivityName, in).Get(ctx, &out)
		if temporal.IsCanceledError(err) {
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
			waitCtx, cancel := workflow.WithCancel(ctx)
			timer := workflow.NewTimer(waitCtx, time.Second)
			selector := workflow.NewSelector(ctx)
			selector.AddFuture(timer, func(workflow.Future) {})
			selector.AddReceive(workflow.GetSignalChannel(ctx, checktemporal.RefreshSignal), func(c workflow.ReceiveChannel, _ bool) { var hint string; c.Receive(ctx, &hint) })
			selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
			selector.Select(ctx)
			cancel()
		}
	}
}

func TestTemporalRealLegacyHistoryReplayAndStartDedup(t *testing.T) {
	c := temporalClient(t)
	h, in := temporalPending(t, "test-lost-response")
	queue := "legacy-" + in.ID
	w := temporalWorker(t, c, queue, checktemporal.Activities{Store: h.jobs, Handler: h.worker.Handler}, true)
	defer w.Stop()
	s := checktemporal.Starter{Client: c, Queue: queue}
	ctx := context.Background()
	if err := s.Start(ctx, in.Execution); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(ctx, in.Execution); err != nil {
		t.Fatal("uncertain start did not deduplicate", err)
	}
	if state := temporalRun(t, c, in.Execution); state.Stage != domain.Confirmed {
		t.Fatal(state)
	}
	before, err := c.DescribeWorkflowExecution(ctx, checktemporal.WorkflowID(in.Execution), "")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Start(ctx, in.Execution); err != nil {
		t.Fatal("closed execution acknowledgment", err)
	}
	after, err := c.DescribeWorkflowExecution(ctx, checktemporal.WorkflowID(in.Execution), "")
	if err != nil || before.WorkflowExecutionInfo.Execution.RunId != after.WorkflowExecutionInfo.Execution.RunId {
		t.Fatal("completed checkout was restarted", err)
	}
	history := temporalHistory(t, c, in.Execution)
	var timers int
	for _, event := range history.Events {
		if event.EventType == enums.EVENT_TYPE_MARKER_RECORDED {
			t.Fatal("legacy fixture unexpectedly has a version marker")
		}
		if event.EventType == enums.EVENT_TYPE_TIMER_STARTED {
			timers++
		}
	}
	if timers == 0 {
		t.Fatal("legacy history did not exercise old durable timer")
	}
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflowWithOptions(checktemporal.Checkout, workflow.RegisterOptions{Name: checktemporal.WorkflowName})
	if err = r.ReplayWorkflowHistory(nil, history); err != nil {
		t.Fatal("current code cannot replay v0 history", err)
	}
	b, err := protojson.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "artifacts", "temporal-v0-history.json")
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	assertTemporalCaptureOnce(t, in.Execution)
}

type lostActivityCheckpoint struct {
	ports.TemporalStore
	stage   string
	blocked chan struct{}
	fired   atomic.Bool
}

func (s *lostActivityCheckpoint) TemporalSave(ctx context.Context, j ports.Job, step int64, next domain.State, retry bool) (ports.TemporalResult, error) {
	if !retry && j.State.Stage == s.stage && s.fired.CompareAndSwap(false, true) {
		close(s.blocked)
		<-ctx.Done() // actual SDK shutdown cancels the in-flight activity.
		return ports.TemporalResult{}, ctx.Err()
	}
	return s.TemporalStore.TemporalSave(ctx, j, step, next, retry)
}

func TestTemporalRealWorkerLossAfterEveryExternalStage(t *testing.T) {
	c := temporalClient(t)
	for _, stage := range []string{domain.Reserving, domain.Capturing, domain.Allocating, domain.Confirming, domain.Compensating} {
		t.Run(stage, func(t *testing.T) {
			method := "test-ok"
			if stage == domain.Compensating {
				method = "test-decline"
			}
			h, in := temporalPending(t, method)
			queue := "restart-" + in.ID
			crash := &lostActivityCheckpoint{TemporalStore: h.jobs, stage: stage, blocked: make(chan struct{})}
			w := temporalWorker(t, c, queue, checktemporal.Activities{Store: crash, Handler: h.worker.Handler}, false)
			if err := (checktemporal.Starter{Client: c, Queue: queue}).Start(context.Background(), in.Execution); err != nil {
				w.Stop()
				t.Fatal(err)
			}
			select {
			case <-crash.blocked:
			case <-time.After(20 * time.Second):
				w.Stop()
				t.Fatal("worker did not reach crash boundary")
			}
			w.Stop()
			// Expire the test lease explicitly instead of spending 30s per case.
			// The real container-kill scenario below waits for natural expiry.
			if _, err := pool.Exec(context.Background(), `UPDATE checkout.jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID); err != nil {
				t.Fatal(err)
			}
			replacement := temporalWorker(t, c, queue, checktemporal.Activities{Store: h.jobs, Handler: h.worker.Handler}, false)
			defer replacement.Stop()
			want := domain.Confirmed
			if stage == domain.Compensating {
				want = domain.Failed
			}
			if state := temporalRun(t, c, in.Execution); state.Stage != want {
				t.Fatal(state)
			}
			if want == domain.Confirmed {
				assertTemporalCaptureOnce(t, in.Execution)
			}
			var steps, ordinal int
			if err := pool.QueryRow(context.Background(), `SELECT temporal_step,(SELECT count(*) FROM checkout.temporal_steps WHERE tenant_id=$1 AND id=$2::uuid) FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID).Scan(&ordinal, &steps); err != nil || steps != ordinal {
				t.Fatal("receipt gap after restart", steps, ordinal, err)
			}
		})
	}
}

func TestTemporalRealNativeCancelCompensatesCapturedPayment(t *testing.T) {
	c := temporalClient(t)
	h, in := temporalPending(t, "test-ok")
	ctx := context.Background()
	a := checktemporal.Activities{Store: h.jobs, Handler: h.worker.Handler}
	// Capture first, then pause at a durable step boundary before allocation.
	for {
		out, err := a.Advance(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		in.Step++
		if out.State.Stage == domain.Allocating {
			break
		}
	}
	control(t, h.jobs, operatorContext(t, in.TenantID), in.TenantID, in.ID, "pause", "pause", 0)
	queue := "native-cancel-" + in.ID
	w := temporalWorker(t, c, queue, a, false)
	defer w.Stop()
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: checktemporal.WorkflowID(in.Execution), TaskQueue: queue}, checktemporal.WorkflowName, in)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.CancelWorkflow(ctx, checktemporal.WorkflowID(in.Execution), ""); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		v, err := h.jobs.Get(ctx, in.TenantID, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		if v.CancelRequested {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native cancellation was not persisted")
		}
		time.Sleep(50 * time.Millisecond)
	}
	control(t, h.jobs, operatorContext(t, in.TenantID), in.TenantID, in.ID, "resume", "resume", 1)
	if state := temporalRun(t, c, in.Execution); state.Stage != domain.Cancelled {
		t.Fatal(state)
	}
	for _, kind := range []string{"capture", "refund"} {
		var count int
		if err = pool.QueryRow(ctx, `SELECT count(*) FROM payments.journal WHERE tenant_id=$1 AND payment_id=$2::uuid AND kind=$3`, in.TenantID, in.ID, kind).Scan(&count); err != nil || count != 1 {
			t.Fatal(fmt.Sprintf("%s not posted exactly once", kind), count, err)
		}
	}
}
