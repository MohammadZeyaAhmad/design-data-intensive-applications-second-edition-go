package main

import (
	"encoding/json"
	"errors"
	"fmt"
	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	pay "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	checkports "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"time"
)

func startCheckout(c *client, method string) (orders.View, string, error) {
	name, err := sku("checkout-")
	if err != nil {
		return orders.View{}, "", err
	}
	_, err = c.expect("PUT", "/v1/catalog/products/"+name, "acme", name+"-product", map[string]any{"name": "Synthetic book", "unit_price_minor": 1250, "currency": "USD", "expected_version": 0}, 200)
	if err != nil {
		return orders.View{}, "", err
	}
	if _, err = c.expect("POST", "/v1/stock/"+name+"/receipts", "acme", name+"-stock", map[string]int{"quantity": 10}, 200); err != nil {
		return orders.View{}, "", err
	}
	body := orderCommand(name, 2)
	b, err := c.expect("POST", "/v1/orders", "acme", name+"-order", body, 201)
	if err != nil {
		return orders.View{}, "", err
	}
	order, err := decodeOrder(b)
	if err != nil {
		return orders.View{}, "", err
	}
	b, err = c.expect("POST", "/v1/catalog/quotes", "acme", name+"-quote", body, 201)
	if err != nil {
		return orders.View{}, "", err
	}
	var q cat.Quote
	if err = json.Unmarshal(b, &q); err != nil {
		return orders.View{}, "", err
	}
	request := map[string]any{"expected_version": 1, "quote_id": q.ID, "payment_method": method}
	b, err = c.expect("POST", "/v1/orders/"+order.ID+"/checkout", "acme", name+"-checkout", request, 202)
	if err != nil {
		return orders.View{}, "", err
	}
	var result orders.View
	if err = json.Unmarshal(b, &result); err != nil {
		return result, "", err
	}
	// Catalog changes after acceptance must not alter the frozen financial amount.
	if _, err = c.expect("PUT", "/v1/catalog/products/"+name, "acme", name+"-repriced", map[string]any{"name": "Synthetic book", "unit_price_minor": 9000, "currency": "USD", "expected_version": 1}, 200); err != nil {
		return result, "", err
	}
	return result, name, nil
}
func waitCheckout(c *client, id string) (checkports.View, error) {
	deadline := time.Now().Add(50 * time.Second)
	for time.Now().Before(deadline) {
		status, b, err := c.request("GET", "/v1/checkouts/"+id, "acme", "", nil)
		if err != nil {
			return checkports.View{}, err
		}
		if status == 200 {
			var view checkports.View
			if err = json.Unmarshal(b, &view); err != nil {
				return view, err
			}
			if view.Status == "confirmed" || view.Status == "failed" || view.Status == "cancelled" {
				return view, nil
			}
		} else if status != 404 {
			return checkports.View{}, fmt.Errorf("checkout read returned %d", status)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return checkports.View{}, errors.New("checkout did not reach a terminal state within 50 seconds")
}
func demoCheckout(c *client) error {
	result, _, err := startCheckout(c, "test-ok")
	if err != nil {
		return err
	}
	view, err := waitCheckout(c, result.ID)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(view, "", "  ")
	fmt.Println(string(b))
	if view.Status != "confirmed" {
		return errors.New("demo checkout failed")
	}
	return nil
}
func verifyCheckout(c *client) error {
	for _, method := range []string{"test-ok", "test-lost-response", "test-decline"} {
		started, sku, err := startCheckout(c, method)
		if err != nil {
			return err
		}
		view, err := waitCheckout(c, started.ID)
		if err != nil {
			return err
		}
		want := "confirmed"
		if method == "test-decline" {
			want = "failed"
		}
		if view.Status != want || view.TotalMinor != 2500 {
			return fmt.Errorf("%s checkout result: %+v", method, view)
		}
		if _, err = c.expect("GET", "/v1/checkouts/"+started.ID, "globex", "", nil, 404); err != nil {
			return err
		}
		b, err := c.expect("GET", "/v1/stock/"+sku, "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var stock domain.StockState
		if err = json.Unmarshal(b, &stock); err != nil {
			return err
		}
		if stock.Reserved != 0 || (want == "confirmed" && stock.Allocated != 2) || (want == "failed" && stock.Available() != 10) {
			return errors.New("checkout violated stock accounting")
		}
		b, err = c.expect("GET", "/v1/payments/"+started.ID, "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var payment pay.Receipt
		if err = json.Unmarshal(b, &payment); err != nil {
			return err
		}
		if payment.AmountMinor != 2500 || payment.Currency != "USD" {
			return errors.New("payment ignored frozen quote")
		}
		b, err = c.expect("GET", "/v1/payments/"+started.ID+"/journal", "acme", "", nil, 200)
		if err != nil {
			return err
		}
		var journal struct {
			Entries []struct {
				Kind   string `json:"kind"`
				Amount int64  `json:"amount_minor"`
			} `json:"entries"`
		}
		if err = json.Unmarshal(b, &journal); err != nil {
			return err
		}
		entries := 1
		if want == "failed" {
			entries = 0
		}
		if len(journal.Entries) != entries {
			return errors.New("duplicate or missing financial entry")
		}
	}
	return report("checkout", map[string]any{"passed": true, "catalog_reprice_preserves_accepted_amount": true, "normal_and_lost_response_capture_once": true, "decline_compensates_inventory": true, "tenant_isolation": true})
}
