package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/operations"
	checkports "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	fulfillports "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
	orderports "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type adminClient struct{ binary, correlation string }

func (a adminClient) call(mode, kind, id string, extra ...string) ([]byte, error) {
	args := []string{mode, "-kind", kind, "-tenant", "acme", "-id", id, "-correlation-id", a.correlation}
	cmd := exec.Command(a.binary, append(args, extra...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	b, err := cmd.Output()
	if err != nil {
		return nil, errors.New(strings.TrimSpace(stderr.String()))
	}
	return b, nil
}
func (a adminClient) control(kind, id, action string, version int64) (api.Result, error) {
	key := id + "-" + action
	deadline := time.Now().Add(20 * time.Second)
	for {
		b, err := a.call("workflow-control", kind, id, "-action", action, "-reason", "investigation", "-key", key, "-expected-control-version", fmt.Sprint(version))
		if err != nil {
			if strings.Contains(err.Error(), api.ErrBusy.Error()) && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return api.Result{}, err
		}
		var out struct {
			Result api.Result `json:"result"`
		}
		if err = json.Unmarshal(b, &out); err != nil {
			return api.Result{}, err
		}
		return out.Result, nil
	}
}
func verifyOperations(c *client) (result error) {
	u, _ := url.Parse(c.base)
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" {
		return errors.New("operator recovery verification requires the local development stack")
	}
	correlation, err := identity.NewID()
	if err != nil {
		return err
	}
	copy := *c
	copy.correlation = correlation
	c = &copy
	binary, err := filepath.Abs("artifacts/admin-verification")
	if err != nil {
		return err
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/admin")
	if err = build.Run(); err != nil {
		return errors.New("could not build local operator command")
	}
	a := adminClient{binary: binary, correlation: correlation}
	if err = compose("stop", "provider"); err != nil {
		return err
	}
	providerStopped := true
	defer func() {
		if providerStopped {
			result = errors.Join(result, compose("start", "provider"))
		}
	}()
	started, _, err := startCheckout(c, "test-lost-response")
	if err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	var pending checkports.View
	for time.Now().Before(deadline) {
		status, b, err := c.request("GET", "/v1/checkouts/"+started.ID, "acme", "", nil)
		if err != nil {
			return err
		}
		if status == 200 && json.Unmarshal(b, &pending) == nil && pending.Status == "capturing" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pending.Status != "capturing" || pending.Metadata.CorrelationID != correlation || pending.Metadata.Actor != "tenant-key:acme" {
		return errors.New("accepted checkout lost its originating provenance")
	}
	paused, err := a.control("checkout", started.ID, "pause", 0)
	if err != nil {
		return err
	}
	if !paused.View.Paused || paused.View.ControlVersion != 1 {
		return errors.New("operator pause did not persist")
	}
	if err = compose("start", "provider"); err != nil {
		return err
	}
	providerStopped = false
	if err = compose("restart", "api", "worker"); err != nil {
		return err
	}
	if err = waitReady(c); err != nil {
		return err
	}
	b, err := c.expect("GET", "/v1/checkouts/"+started.ID, "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var held checkports.View
	if err = json.Unmarshal(b, &held); err != nil {
		return err
	}
	if !held.Paused || held.Status != "capturing" || held.Metadata != pending.Metadata {
		return errors.New("restart changed paused progress or accepted provenance")
	}
	replay, err := a.control("checkout", started.ID, "pause", 0)
	if err != nil || replay.AuditID != paused.AuditID {
		return errors.New("control retry created a second audit identity")
	}
	if _, err = a.control("checkout", started.ID, "resume", 1); err != nil {
		return err
	}
	final, err := waitCheckout(c, started.ID)
	if err != nil {
		return err
	}
	if final.Status != "confirmed" {
		return errors.New("resumed checkout did not confirm")
	}
	for version := int64(1); version <= 2; version++ {
		b, err = a.call("workflow-history", "checkout", started.ID, "-after-version", fmt.Sprint(version-1), "-limit", "1")
		if err != nil {
			return err
		}
		var page api.Page
		if err = json.Unmarshal(b, &page); err != nil {
			return err
		}
		if len(page.Entries) != 1 || page.NextAfterVersion != version || page.Entries[0].Metadata.Actor != "operator:local" || page.Entries[0].Metadata.CorrelationID != correlation {
			return errors.New("operator audit pagination or provenance changed")
		}
	}
	b, err = c.expect("GET", "/v1/orders/"+started.OrderID+"/events", "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var events orderports.EventPage
	if err = json.Unmarshal(b, &events); err != nil {
		return err
	}
	if len(events.Events) != 3 {
		return errors.New("unexpected order facts after recovery")
	}
	for _, e := range events.Events[1:] {
		if e.Metadata.CorrelationID != correlation || e.Metadata.OriginRequestID != pending.Metadata.OriginRequestID {
			return errors.New("asynchronous order lineage changed")
		}
	}
	if events.Events[2].Metadata.Actor != "worker:checkout" {
		return errors.New("confirmation executor not recorded")
	}
	b, err = c.expect("GET", "/v1/payments/"+started.ID+"/journal", "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var journal struct {
		Entries []struct {
			Metadata envelope.Metadata `json:"metadata"`
		} `json:"entries"`
	}
	if err = json.Unmarshal(b, &journal); err != nil {
		return err
	}
	if len(journal.Entries) != 1 || journal.Entries[0].Metadata.CorrelationID != correlation || journal.Entries[0].Metadata.OriginRequestID != pending.Metadata.OriginRequestID {
		return errors.New("payment replay lost identity or lineage")
	}
	warehouseOrder, _, err := startWarehouseCheckout(c)
	if err != nil {
		return err
	}
	shipments, err := waitShipments(c, warehouseOrder.OrderID, "ready")
	if err != nil {
		return err
	}
	for _, s := range shipments {
		if _, err = a.control("shipment", s.ID, "pause", 0); err != nil {
			return err
		}
		for i, action := range []string{"pick", "pack", "dispatch"} {
			input := map[string]any{"expected_version": i + 1}
			status := 200
			if action == "dispatch" {
				input["tracking_reference"] = "OPS-" + s.ID
				status = 202
			}
			if _, err = c.expect("POST", "/v1/shipments/"+s.ID+"/"+action, "acme", s.ID+"-"+action, input, status); err != nil {
				return err
			}
		}
	}
	pausedShipments, err := waitShipments(c, warehouseOrder.OrderID, "dispatching")
	if err != nil {
		return err
	}
	for _, s := range pausedShipments {
		if !s.Paused || s.Dispatched != 0 || s.DispatchMetadata.Actor != "tenant-key:acme" || s.DispatchMetadata.CorrelationID != correlation {
			return errors.New("paused shipment executed or lost dispatch provenance")
		}
	}
	for _, s := range shipments {
		if _, err = a.control("shipment", s.ID, "resume", 1); err != nil {
			return err
		}
	}
	shipments, err = waitShipments(c, warehouseOrder.OrderID, "shipped")
	if err != nil {
		return err
	}
	for _, s := range shipments {
		b, err = c.expect("GET", "/v1/shipments/"+s.ID+"/events", "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var history []fulfillports.Event
		if err = json.Unmarshal(b, &history); err != nil {
			return err
		}
		last := history[len(history)-1].Metadata
		if last.Actor != "worker:fulfillment" || last.CorrelationID != correlation || last.OriginRequestID != s.DispatchMetadata.OriginRequestID {
			return errors.New("handoff did not resume persisted dispatch lineage")
		}
	}
	return report("operations", map[string]any{"passed": true, "correlation_survives_async_work": true, "origin_and_executor_recorded": true, "pause_survives_process_restart": true, "control_retry_has_one_audit_receipt": true, "audit_history_keyset_pagination": true, "one_capture_after_recovery": true, "warehouse_dispatch_resumes": true})
}
