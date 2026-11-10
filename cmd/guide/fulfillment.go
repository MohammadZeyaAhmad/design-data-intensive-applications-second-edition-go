package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	fd "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/domain"
	fp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
	id "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"net/url"
	"time"
)

func startWarehouseCheckout(c *client) (orders.View, []string, error) {
	base, err := sku("fulfillment-")
	if err != nil {
		return orders.View{}, nil, err
	}
	skus := []string{base + "-book", base + "-pen"}
	for i, name := range skus {
		if _, err = c.expect("PUT", "/v1/catalog/products/"+name, "acme", name+"-product", map[string]any{"name": "Synthetic warehouse item", "unit_price_minor": 100, "currency": "USD", "expected_version": 0}, 200); err != nil {
			return orders.View{}, nil, err
		}
		wid, quantity := "east", 4
		if i == 1 {
			wid, quantity = "west", 6
		}
		if _, err = c.expect("POST", "/v1/stock/"+name+"/receipts", "acme", name+"-stock", map[string]any{"quantity": quantity, "warehouse_id": wid}, 200); err != nil {
			return orders.View{}, nil, err
		}
	}
	// A second location for the same SKU must remain untouched by east's line.
	if _, err = c.expect("POST", "/v1/stock/"+skus[0]+"/receipts", "acme", base+"-spare", map[string]any{"quantity": 7, "warehouse_id": "west"}, 200); err != nil {
		return orders.View{}, nil, err
	}
	lines := map[string]any{"lines": []map[string]any{{"sku": skus[0], "quantity": 2}, {"sku": skus[1], "quantity": 3}}}
	b, err := c.expect("POST", "/v1/orders", "acme", base+"-order", lines, 201)
	if err != nil {
		return orders.View{}, nil, err
	}
	order, err := decodeOrder(b)
	if err != nil {
		return orders.View{}, nil, err
	}
	b, err = c.expect("POST", "/v1/catalog/quotes", "acme", base+"-quote", lines, 201)
	if err != nil {
		return orders.View{}, nil, err
	}
	var q cat.Quote
	if err = json.Unmarshal(b, &q); err != nil {
		return orders.View{}, nil, err
	}
	b, err = c.expect("POST", "/v1/orders/"+order.ID+"/checkout", "acme", base+"-checkout", map[string]any{"expected_version": 1, "quote_id": q.ID, "payment_method": "test-ok"}, 202)
	if err != nil {
		return orders.View{}, nil, err
	}
	var result orders.View
	if err = json.Unmarshal(b, &result); err != nil {
		return result, nil, err
	}
	terminal, err := waitCheckout(c, result.ID)
	if err != nil {
		return result, nil, err
	}
	if terminal.Status != "confirmed" {
		return result, nil, errors.New("warehouse checkout failed")
	}
	return result, skus, nil
}
func waitShipments(c *client, order, status string) ([]fp.View, error) {
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		b, err := c.expect("GET", "/v1/orders/"+order+"/fulfillment", "acme", "", nil, 200)
		if err != nil {
			return nil, err
		}
		var views []fp.View
		if err = json.Unmarshal(b, &views); err != nil {
			return nil, err
		}
		ready := len(views) == 2
		for _, v := range views {
			ready = ready && v.Status == status
		}
		if ready {
			return views, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("shipments did not reach %s", status)
}
func verifyFulfillment(c *client, recovery bool) (result error) {
	if recovery {
		u, _ := url.Parse(c.base)
		if u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" {
			return errors.New("fulfillment recovery requires the local development stack")
		}
	}
	order, skus, err := startWarehouseCheckout(c)
	if err != nil {
		return err
	}
	shipments, err := waitShipments(c, order.OrderID, fd.Ready)
	if err != nil {
		return err
	}
	if shipments[0].WarehouseID != "east" || shipments[1].WarehouseID != "west" {
		return errors.New("warehouse shipment grouping changed")
	}
	stopped := false
	defer func() {
		if stopped {
			result = errors.Join(result, compose("start", "worker"))
		}
	}()
	if recovery {
		if err = compose("stop", "worker"); err != nil {
			return err
		}
		stopped = true
	}
	saved := map[string][]byte{}
	for _, s := range shipments {
		path := "/v1/shipments/" + s.ID
		if _, err = c.expect("GET", path, "globex", "", nil, 404); err != nil {
			return err
		}
		if _, err = c.expect("POST", path+"/pack", "acme", s.ID+"-pack", map[string]any{"expected_version": 1}, 409); err != nil {
			return err
		}
		for i, action := range []string{"pick", "pack", "dispatch"} {
			body := map[string]any{"expected_version": i + 1}
			code := 200
			if action == "dispatch" {
				body["tracking_reference"] = "TEST-" + s.ID
				code = 202
			}
			b, err := c.expect("POST", path+"/"+action, "acme", s.ID+"-"+action, body, code)
			if err != nil {
				return err
			}
			if action == "dispatch" {
				saved[s.ID] = b
			}
		}
		if _, err = c.expect("POST", path+"/dispatch", "acme", s.ID+"-dispatch", map[string]any{"expected_version": 3, "tracking_reference": "CHANGED"}, 409); err != nil {
			return err
		}
	}
	if recovery {
		if _, err = waitShipments(c, order.OrderID, fd.Dispatching); err != nil {
			return err
		}
		if err = compose("restart", "api"); err != nil {
			return err
		}
		if err = waitReady(c); err != nil {
			return err
		}
	}
	for _, s := range shipments {
		b, err := c.expect("POST", "/v1/shipments/"+s.ID+"/dispatch", "acme", s.ID+"-dispatch", map[string]any{"expected_version": 3, "tracking_reference": "TEST-" + s.ID}, 202)
		if err != nil {
			return err
		}
		if !bytes.Equal(b, saved[s.ID]) {
			return errors.New("dispatch replay did not preserve its saved response")
		}
	}
	if stopped {
		if err = compose("start", "worker"); err != nil {
			return err
		}
		stopped = false
	}
	shipments, err = waitShipments(c, order.OrderID, fd.Shipped)
	if err != nil {
		return err
	}
	for _, s := range shipments {
		if s.Dispatched != 1 || s.Version != 5 {
			return errors.New("shipment handoff was not applied once")
		}
		if _, err = c.expect("POST", "/v1/shipments/"+s.ID+"/deliver", "acme", s.ID+"-deliver", map[string]any{"expected_version": s.Version}, 200); err != nil {
			return err
		}
		b, err := c.expect("GET", "/v1/shipments/"+s.ID+"/events", "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var events []fp.Event
		if err = json.Unmarshal(b, &events); err != nil {
			return err
		}
		if len(events) != 6 || events[5].State.Status != fd.Delivered {
			return errors.New("shipment history is incomplete")
		}
	}
	for i, name := range skus {
		wid, want := "east", int64(2)
		if i == 1 {
			wid, want = "west", 3
		}
		path := "/v1/stock/" + name
		b, err := c.expect("GET", path+"?warehouse_id="+wid, "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var state id.StockState
		if err = json.Unmarshal(b, &state); err != nil {
			return err
		}
		if state.OnHand != want || state.Allocated != 0 || state.Reserved != 0 || state.Version != 4 {
			return errors.New("warehouse stock was removed more than once")
		}
		deadline := time.Now().Add(10 * time.Second)
		matched := false
		for time.Now().Before(deadline) {
			code, b, err := c.request("GET", path+"/projection?warehouse_id="+wid, "acme", "", nil)
			if err != nil {
				return err
			}
			var projected id.StockState
			if code == 200 && json.Unmarshal(b, &projected) == nil && projected == state {
				matched = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !matched {
			return errors.New("warehouse projection did not catch up")
		}
	}
	b, err := c.expect("GET", "/v1/stock/"+skus[0]+"?warehouse_id=west", "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var spare id.StockState
	if err = json.Unmarshal(b, &spare); err != nil {
		return err
	}
	if spare.OnHand != 7 || spare.Version != 1 {
		return errors.New("shipping modified another warehouse")
	}
	b, err = c.expect("GET", "/v1/payments/"+order.ID+"/journal", "acme", "", nil, 200)
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
		return errors.New("fulfillment changed payment effects")
	}
	name := "fulfillment"
	if recovery {
		name = "fulfillment-recovery"
	}
	return report(name, map[string]any{"passed": true, "warehouse_shipments": 2, "stock_removed_once": true, "other_location_unchanged": true, "tenant_isolation": true, "saved_dispatch_replay": true, "shipment_history": true, "worker_and_api_restarted": recovery})
}
