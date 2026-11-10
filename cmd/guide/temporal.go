package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"time"

	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	checktemporal "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/temporal"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	temporalclient "go.temporal.io/sdk/client"
)

func temporalPendingView(c *client, id string, condition func(ports.View) bool) (ports.View, error) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		status, b, err := c.request("GET", "/v1/checkouts/"+id, "acme", "", nil)
		if err != nil {
			return ports.View{}, err
		}
		var v ports.View
		if status == 200 && json.Unmarshal(b, &v) == nil && condition(v) {
			return v, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ports.View{}, errors.New("Temporal checkout did not reach the recovery boundary")
}
func temporalCaptureOnce(c *client, started orders.View) error {
	v, err := waitCheckout(c, started.ID)
	if err != nil {
		return err
	}
	if v.Engine != "temporal" || v.Status != "confirmed" || v.TotalMinor != 2500 {
		return errors.New("Temporal recovery changed the accepted outcome")
	}
	b, err := c.expect("GET", "/v1/payments/"+started.ID+"/journal", "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var j struct {
		Entries []struct {
			Kind string `json:"kind"`
		} `json:"entries"`
	}
	if err = json.Unmarshal(b, &j); err != nil {
		return err
	}
	if len(j.Entries) != 1 || j.Entries[0].Kind != "capture" {
		return errors.New("Temporal recovery duplicated a financial effect")
	}
	return nil
}

func verifyTemporal(c *client) (result error) {
	u, _ := url.Parse(c.base)
	if os.Getenv("ATLAS_CHECKOUT_ENGINE") != "temporal" || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1") {
		return errors.New("Temporal recovery requires the local Temporal profile")
	}
	// Restore every service even when a recovery assertion fails.
	defer func() {
		result = errors.Join(result, compose("up", "-d", "--wait", "temporal"))
		result = errors.Join(result, compose("start", "provider", "temporal-dispatcher", "temporal-worker"))
	}()
	if err := compose("stop", "temporal-dispatcher", "temporal-worker", "temporal"); err != nil {
		return err
	}
	accepted, sku, err := startCheckout(c, "test-ok")
	if err != nil {
		return err
	}
	v, err := temporalPendingView(c, accepted.ID, func(v ports.View) bool { return v.Engine == "temporal" })
	if err != nil {
		return err
	}
	if v.Status != "reserving" {
		return errors.New("PostgreSQL worker advanced a Temporal-owned checkout")
	}
	if _, err = c.expect("POST", "/v1/orders/"+accepted.OrderID+"/checkout", "acme", sku+"-checkout", map[string]any{"expected_version": 1, "quote_id": accepted.Quote.ID, "payment_method": "test-ok"}, 202); err != nil {
		return err
	}
	if err = compose("up", "-d", "--wait", "temporal"); err != nil {
		return err
	}
	if err = compose("start", "temporal-dispatcher", "temporal-worker"); err != nil {
		return err
	}
	if err = temporalCaptureOnce(c, accepted); err != nil {
		return err
	}

	if err = compose("stop", "provider"); err != nil {
		return err
	}
	pending, _, err := startCheckout(c, "test-ok")
	if err != nil {
		return err
	}
	if _, err = temporalPendingView(c, pending.ID, func(v ports.View) bool { return v.Status == "capturing" && v.Attempts > 0 }); err != nil {
		return err
	}
	address := os.Getenv("ATLAS_TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7233"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	tc, err := temporalclient.DialContext(ctx, temporalclient.Options{HostPort: address})
	cancel()
	if err != nil {
		return err
	}
	defer tc.Close()
	e := ports.Execution{TenantID: "acme", ID: pending.ID}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	before, err := tc.DescribeWorkflowExecution(ctx, checktemporal.WorkflowID(e), "")
	cancel()
	if err != nil {
		return err
	}
	if err = compose("kill", "-s", "SIGKILL", "temporal-worker", "temporal"); err != nil {
		return err
	}
	if err = compose("restart", "api"); err != nil {
		return err
	}
	if err = waitReady(c); err != nil {
		return err
	}
	if err = compose("up", "-d", "--wait", "temporal"); err != nil {
		return err
	}
	if err = compose("start", "provider", "temporal-worker"); err != nil {
		return err
	}
	if err = temporalCaptureOnce(c, pending); err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	// A legitimate continuation may have created a newer run during recovery.
	// Verify that the original persisted run survives, independent of that.
	after, err := tc.DescribeWorkflowExecution(ctx, checktemporal.WorkflowID(e), before.WorkflowExecutionInfo.Execution.RunId)
	cancel()
	if err != nil {
		return err
	}
	if before.WorkflowExecutionInfo.Execution.RunId != after.WorkflowExecutionInfo.Execution.RunId {
		return errors.New("server restart lost the workflow run")
	}

	// The provider sleeps after accepting the request and keeps working even
	// when the caller dies. Kill the real worker while that RPC is in flight.
	slow, _, err := startCheckout(c, "test-slow")
	if err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	inFlight := false
	for time.Now().Before(deadline) {
		status, b, err := c.request("GET", "/v1/payments/"+slow.ID, "acme", "", nil)
		if err != nil {
			return err
		}
		var payment struct {
			Status string `json:"status"`
		}
		if status == 200 && json.Unmarshal(b, &payment) == nil && payment.Status == "pending" {
			inFlight = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !inFlight {
		return errors.New("could not observe the in-flight provider request")
	}
	if err = compose("kill", "-s", "SIGKILL", "temporal-worker"); err != nil {
		return err
	}
	if err = compose("start", "temporal-worker"); err != nil {
		return err
	}
	if err = temporalCaptureOnce(c, slow); err != nil {
		return err
	}
	return report("temporal-recovery", map[string]any{"passed": true, "accepted_during_temporal_outage": true, "postgres_worker_respects_owner": true, "server_kill_preserves_run": true, "worker_kill_during_capture_recovers": true, "natural_lease_expiry": true, "capture_posted_once": true})
}
