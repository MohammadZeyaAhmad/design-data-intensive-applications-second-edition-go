package main

import (
	"encoding/json"
	"errors"
	checkports "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"net/url"
	"time"
)

func verifyCheckoutRecovery(c *client) (result error) {
	u, _ := url.Parse(c.base)
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" {
		return errors.New("checkout recovery is restricted to the local development stack")
	}
	if err := compose("stop", "provider"); err != nil {
		return err
	}
	restored := false
	defer func() {
		if !restored {
			result = errors.Join(result, compose("start", "provider", "worker"))
		}
	}()
	started, sku, err := startCheckout(c, "test-ok")
	if err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	pending := false
	for time.Now().Before(deadline) {
		status, b, err := c.request("GET", "/v1/checkouts/"+started.ID, "acme", "", nil)
		if err != nil {
			return err
		}
		if status == 200 {
			var view checkports.View
			if err = json.Unmarshal(b, &view); err != nil {
				return err
			}
			if view.Status == "capturing" {
				pending = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !pending {
		return errors.New("provider outage did not leave a retryable capture")
	}
	if err = compose("stop", "worker"); err != nil {
		return err
	}
	if err = compose("start", "provider", "worker"); err != nil {
		return err
	}
	restored = true
	view, err := waitCheckout(c, started.ID)
	if err != nil {
		return err
	}
	if view.Status != "confirmed" || view.TotalMinor != 2500 {
		return errors.New("pending checkout did not recover after process restart")
	}
	if _, err = c.expect("POST", "/v1/orders/"+started.OrderID+"/checkout", "acme", sku+"-checkout", map[string]any{"expected_version": 1, "quote_id": started.Quote.ID, "payment_method": "test-ok"}, 202); err != nil {
		return err
	}
	b, err := c.expect("GET", "/v1/payments/"+started.ID+"/journal", "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var journal struct {
		Entries []json.RawMessage `json:"entries"`
	}
	if err = json.Unmarshal(b, &journal); err != nil {
		return err
	}
	if len(journal.Entries) != 1 {
		return errors.New("recovered checkout produced duplicate payment effects")
	}
	return report("checkout-recovery", map[string]any{"passed": true, "provider_outage_remains_pending": true, "worker_restart_resumes_checkout": true, "capture_posted_once": true, "accepted_checkout_replay": true})
}
