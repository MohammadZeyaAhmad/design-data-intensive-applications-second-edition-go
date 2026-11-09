package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	orderports "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
)

func orderCommand(sku string, q int64) map[string]any {
	return map[string]any{"lines": []orders.Line{{SKU: sku, Quantity: q}}}
}
func decodeOrder(b []byte) (orders.State, error) {
	var state orders.State
	err := json.Unmarshal(b, &state)
	return state, err
}
func demoOrders(c *client) error {
	key, err := sku("order-demo-")
	if err != nil {
		return err
	}
	b, err := c.expect("POST", "/v1/orders", "acme", key, orderCommand("BOOK", 2), 201)
	if err != nil {
		return err
	}
	state, err := decodeOrder(b)
	if err != nil {
		return err
	}
	b, err = c.expect("POST", "/v1/orders/"+state.ID+"/cancel", "acme", key+"-cancel", map[string]any{"expected_version": 1, "reason": "customer_requested"}, 200)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
func orderAdmin(mode, id string) error {
	cmd := exec.Command("go", "run", "./cmd/admin", mode, "-tenant", "acme", "-id", id)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
func waitOrder(c *client, want orders.State) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		status, b, err := c.request("GET", fmt.Sprintf("/v1/orders/%s/projection?min_version=%d", want.ID, want.Version), "acme", "", nil)
		if err != nil {
			return err
		}
		if status == 200 {
			got, err := decodeOrder(b)
			if err != nil {
				return err
			}
			if reflect.DeepEqual(got, want) {
				return nil
			}
		} else if status != 404 && status != 409 {
			return fmt.Errorf("order projection returned %d", status)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("order projection did not catch up")
}
func verifyOrders(c *client) error {
	key, err := sku("order-verify-")
	if err != nil {
		return err
	}
	b, err := c.expect("POST", "/v1/orders", "acme", key, orderCommand("BOOK", 2), 201)
	if err != nil {
		return err
	}
	state, err := decodeOrder(b)
	if err != nil {
		return err
	}
	replay, err := c.expect("POST", "/v1/orders", "acme", key, orderCommand("BOOK", 2), 201)
	if err != nil || !bytes.Equal(b, replay) {
		return errors.New("order placement replay changed")
	}
	if _, err = c.expect("GET", "/v1/orders/"+state.ID, "globex", "", nil, 404); err != nil {
		return err
	}
	var wins, conflicts atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, err := c.request("POST", "/v1/orders/"+state.ID+"/amend", "acme", fmt.Sprintf("%s-amend-%d", key, i), map[string]any{"expected_version": 1, "lines": []orders.Line{{SKU: "BOOK", Quantity: int64(i + 1)}}})
			if err != nil {
				errs <- err
				return
			}
			switch status {
			case 200:
				wins.Add(1)
			case 409:
				conflicts.Add(1)
			default:
				errs <- fmt.Errorf("unexpected order amendment status %d", status)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	if wins.Load() != 1 || conflicts.Load() != 19 {
		return fmt.Errorf("stream race admitted %d amendments, rejected %d", wins.Load(), conflicts.Load())
	}
	if err = orderAdmin("snapshot-order", state.ID); err != nil {
		return err
	}
	b, err = c.expect("POST", "/v1/orders/"+state.ID+"/cancel", "acme", key+"-cancel", map[string]any{"expected_version": 2, "reason": "customer_requested"}, 200)
	if err != nil {
		return err
	}
	state, err = decodeOrder(b)
	if err != nil {
		return err
	}
	if err = orderAdmin("rebuild-order", state.ID); err != nil {
		return err
	}
	if err = waitOrder(c, state); err != nil {
		return err
	}
	var versions []int64
	var after int64
	for range 4 {
		b, err = c.expect("GET", fmt.Sprintf("/v1/orders/%s/events?limit=1&after_version=%d", state.ID, after), "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var page orderports.EventPage
		if err = json.Unmarshal(b, &page); err != nil {
			return err
		}
		if len(page.Events) == 0 {
			break
		}
		versions = append(versions, page.Events[0].Version)
		after = page.NextAfterVersion
	}
	if !reflect.DeepEqual(versions, []int64{1, 2, 3}) {
		return errors.New("history pagination skipped or duplicated events")
	}
	return report("orders", map[string]any{"passed": true, "competing_amendments": 20, "accepted": wins.Load(), "version_conflicts": conflicts.Load(), "event_versions": versions, "snapshot_and_rebuild": true, "tenant_isolation": true})
}

func verifyLifecycle(c *client) error {
	sku, err := sku("lifecycle-")
	if err != nil {
		return err
	}
	if _, err = c.expect("POST", "/v1/stock/"+sku+"/receipts", "acme", sku+"-receipt", map[string]int{"quantity": 5}, 200); err != nil {
		return err
	}
	b, err := c.expect("POST", "/v1/reservations", "acme", sku+"-expiring", map[string]any{"sku": sku, "quantity": 2, "ttl_seconds": 1}, 201)
	if err != nil {
		return err
	}
	var out ports.Outcome
	if err = json.Unmarshal(b, &out); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	expired := false
	for time.Now().Before(deadline) {
		b, err = c.expect("GET", "/v1/reservations/"+out.Reservation.ID, "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var r domain.ReservationState
		if err = json.Unmarshal(b, &r); err != nil {
			return err
		}
		if r.Status == domain.ReservationExpired {
			expired = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !expired {
		return errors.New("expiry worker did not release the due hold")
	}
	if _, err = c.expect("POST", "/v1/reservations/"+out.Reservation.ID+"/commit", "acme", sku+"-late", nil, 409); err != nil {
		return err
	}
	b, err = c.expect("POST", "/v1/reservations", "acme", sku+"-hold", map[string]any{"sku": sku, "quantity": 2}, 201)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &out); err != nil {
		return err
	}
	if _, err = c.expect("POST", "/v1/reservations/"+out.Reservation.ID+"/commit", "acme", sku+"-commit", nil, 200); err != nil {
		return err
	}
	b, err = c.expect("POST", "/v1/reservations/"+out.Reservation.ID+"/reverse", "acme", sku+"-reverse", nil, 200)
	if err != nil {
		return err
	}
	replayed, err := c.expect("POST", "/v1/reservations/"+out.Reservation.ID+"/reverse", "acme", sku+"-reverse", nil, 200)
	if err != nil || !bytes.Equal(b, replayed) {
		return errors.New("reversal replay changed the outcome")
	}
	if err = json.Unmarshal(b, &out); err != nil {
		return err
	}
	if out.Stock.Available() != 5 || out.Stock.Reserved != 0 || out.Stock.Allocated != 0 || out.Stock.Version != 6 {
		return errors.New("expiry/reversal changed inventory incorrectly")
	}
	if err = waitProjection(c, sku, out.Stock); err != nil {
		return err
	}
	return report("inventory-lifecycle", map[string]any{"passed": true, "expired_hold_released": true, "late_commit_rejected": true, "explicit_reversal_once": true, "projection_matches_authority": true})
}
