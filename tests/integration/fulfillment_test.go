//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	inv "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/inventory"
	catdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/advance"
	fulfilldb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/adapters/postgres"
	fd "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/features/changeshipment"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/features/dispatch"
	fp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/ports"
	fr "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/runtime"
	id "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/receivestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/reservestock"
	od "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	op "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"sync"
	"sync/atomic"
	"testing"
)

func receiveAt(t *testing.T, tenant, sku, warehouse, key string, q int64) {
	t.Helper()
	if _, _, err := (receivestock.Handler{Store: store}).Handle(context.Background(), tenant, key, receivestock.Command{SKU: sku, WarehouseID: warehouse, Quantity: q}); err != nil {
		t.Fatal(err)
	}
}
func newID(t *testing.T) string {
	t.Helper()
	id, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func fulfillStore() *fulfilldb.Store { return &fulfilldb.Store{Pool: pool} }
func changeShipment(t *testing.T, s *fulfilldb.Store, tenant, id, action string, version int64) fd.State {
	t.Helper()
	c := changeshipment.Command{ExpectedVersion: version}
	if action == "dispatch" {
		c.TrackingReference = "TEST-TRACK"
	}
	out, _, err := (changeshipment.Handler{Store: s}).Handle(context.Background(), tenant, action+"-"+id, id, action, c)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func confirmedShipment(t *testing.T) (*commerce, *fulfilldb.Store, string, fp.View) {
	t.Helper()
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	product(t, h, tenant, "BOOK", "product-book", 100, 0)
	product(t, h, tenant, "PEN", "product-pen", 50, 0)
	receiveAt(t, tenant, "BOOK", "east", "book-stock", 10)
	receiveAt(t, tenant, "PEN", "east", "pen-stock", 10)
	v, _ := beginCheckout(t, h, tenant, "test-ok", []catdomain.Line{{SKU: "BOOK", Quantity: 2}, {SKU: "PEN", Quantity: 3}})
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
	s := fulfillStore()
	if sent, err := h.orders.DispatchFulfillmentOne(context.Background(), s); err != nil || !sent {
		t.Fatal("fulfillment dispatch", sent, err)
	}
	views, err := s.ForOrder(context.Background(), tenant, v.OrderID)
	if err != nil || len(views) != 1 {
		t.Fatal("shipment grouping", views, err)
	}
	return h, s, tenant, views[0]
}

func TestWarehouseReservationIsolationAndProjection(t *testing.T) {
	tenant := tenant(t)
	ctx := context.Background()
	receiveAt(t, tenant, "BOOK", "east", "east-stock", 3)
	receiveAt(t, tenant, "BOOK", "west", "west-stock", 5)
	receive(t, tenant, "BOOK", "default-stock", 2)
	var accepted, rejected atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := (reservestock.Handler{Store: store}).Handle(ctx, tenant, fmt.Sprintf("reserve-%d", i), reservestock.Command{SKU: "BOOK", WarehouseID: "east", Quantity: 1})
			if err == nil {
				accepted.Add(1)
			} else if errors.Is(err, id.ErrInsufficientStock) {
				rejected.Add(1)
			} else {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if accepted.Load() != 3 || rejected.Load() != 37 {
		t.Fatal("warehouse oversell", accepted.Load(), rejected.Load())
	}
	drain(t)
	for _, wid := range []string{"east", "west", ""} {
		stock, err := store.StockAt(ctx, tenant, "BOOK", wid)
		if err != nil {
			t.Fatal(err)
		}
		view, err := store.ProjectionAt(ctx, tenant, "BOOK", wid)
		if err != nil || stock != view {
			t.Fatal("warehouse projection mismatch", wid, stock, view, err)
		}
		if wid != "east" && stock.Reserved != 0 {
			t.Fatal("reservation spilled warehouses")
		}
	}
	if _, _, err := (receivestock.Handler{Store: store}).Handle(ctx, tenant, "east-stock", receivestock.Command{SKU: "BOOK", WarehouseID: "west", Quantity: 3}); !errors.Is(err, id.ErrIdempotencyConflict) {
		t.Fatal("warehouse changed under existing command", err)
	}
}

func TestCheckoutSourcingPinsOneWarehousePerLine(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx := context.Background()
	step := newID(t)
	receiveAt(t, tenant, "BOOK", "east", "east", 1)
	receiveAt(t, tenant, "BOOK", "west", "west", 2)
	if _, err := h.inventory.Reserve(ctx, tenant, step, inv.Line{SKU: "BOOK", Quantity: 3}, 120); !errors.Is(err, inv.ErrUnavailable) {
		t.Fatal("single line split across warehouses", err)
	}
	r, err := h.inventory.Reserve(ctx, tenant, step, inv.Line{SKU: "BOOK", Quantity: 2}, 120)
	if err != nil || r.WarehouseID != "west" {
		t.Fatal("wrong source", r, err)
	}
	receiveAt(t, tenant, "BOOK", "east", "more-east", 5)
	r, err = h.inventory.Reserve(ctx, tenant, step, inv.Line{SKU: "BOOK", Quantity: 2}, 120)
	if err != nil || r.WarehouseID != "west" {
		t.Fatal("retry changed source", r, err)
	}
	if err = h.inventory.Close(ctx, tenant, step, inv.Line{SKU: "BOOK", Quantity: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.inventory.Reserve(ctx, tenant, step, inv.Line{SKU: "BOOK", Quantity: 2}, 120); !errors.Is(err, inv.ErrClosed) {
		t.Fatal("closed intent reopened elsewhere", err)
	}
	for _, wid := range []string{"east", "west"} {
		stock, err := store.StockAt(ctx, tenant, "BOOK", wid)
		if err != nil || stock.Reserved != 0 {
			t.Fatal("warehouse compensation failed", stock, err)
		}
	}
}

type lostFulfillmentAck struct{ target api.Sink }

func (s lostFulfillmentAck) Submit(ctx context.Context, r api.Request) error {
	if err := s.target.Submit(ctx, r); err != nil {
		return err
	}
	return errors.New("injected lost fulfillment acknowledgment")
}
func TestConfirmedOrderDispatchRedeliveryCreatesWarehouseShipmentsOnce(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx := context.Background()
	product(t, h, tenant, "BOOK", "book", 100, 0)
	product(t, h, tenant, "PEN", "pen", 50, 0)
	receiveAt(t, tenant, "BOOK", "east", "book-stock", 4)
	receiveAt(t, tenant, "PEN", "west", "pen-stock", 6)
	v, _ := beginCheckout(t, h, tenant, "test-ok", []catdomain.Line{{SKU: "BOOK", Quantity: 2}, {SKU: "PEN", Quantity: 3}})
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
	s := fulfillStore()
	if sent, err := h.orders.DispatchFulfillmentOne(ctx, lostFulfillmentAck{target: s}); !sent || err == nil {
		t.Fatal("lost acknowledgment not injected")
	}
	if sent, err := h.orders.DispatchFulfillmentOne(ctx, s); !sent || err != nil {
		t.Fatal("redelivery", sent, err)
	}
	views, err := s.ForOrder(ctx, tenant, v.OrderID)
	if err != nil || len(views) != 2 || views[0].WarehouseID != "east" || views[1].WarehouseID != "west" {
		t.Fatal("wrong shipment groups", views, err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM fulfillment.inbox WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 1 {
		t.Fatal("duplicate inbox", n, err)
	}
	var body []byte
	if err = pool.QueryRow(ctx, `SELECT body FROM ordering.fulfillment_outbox WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID).Scan(&body); err != nil {
		t.Fatal(err)
	}
	var request api.Request
	if err = json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.Submit(ctx, request) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.Get(ctx, commerceTenant(t), views[0].ID); !errors.Is(err, fd.ErrNotFound) {
		t.Fatal("cross-tenant shipment exposed", err)
	}
}

func TestShipmentExpectedVersionRaceAndSavedResponse(t *testing.T) {
	_, s, tenant, v := confirmedShipment(t)
	ctx := context.Background()
	h := changeshipment.Handler{Store: s}
	var won, conflicts atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := h.Handle(ctx, tenant, fmt.Sprintf("pick-%d", i), v.ID, "pick", changeshipment.Command{ExpectedVersion: 1})
			if err == nil {
				won.Add(1)
			} else if errors.Is(err, fd.ErrConflict) {
				conflicts.Add(1)
			} else {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if won.Load() != 1 || conflicts.Load() != 19 {
		t.Fatal("operator race", won.Load(), conflicts.Load())
	}
	packed := changeShipment(t, s, tenant, v.ID, "pack", 2)
	changeShipment(t, s, tenant, v.ID, "dispatch", 3)
	replay, replayed, err := h.Handle(ctx, tenant, "pack-"+v.ID, v.ID, "pack", changeshipment.Command{ExpectedVersion: 2})
	if err != nil || !replayed || replay.Version != packed.Version || replay.Status != fd.Packed {
		t.Fatal("saved response changed", replay, replayed, err)
	}
	if _, _, err = h.Handle(ctx, tenant, "pack-"+v.ID, v.ID, "pack", changeshipment.Command{ExpectedVersion: 3}); !errors.Is(err, commands.ErrConflict) {
		t.Fatal("changed retry accepted", err)
	}
	worker := fr.Worker{Store: s, Handler: dispatch.Handler{Inventory: commerceHarness(t).inventory}}
	for range 2 {
		if _, err = worker.ProcessOne(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestShipmentCrashAfterStockCommitAndStaleCheckpoint(t *testing.T) {
	h, s, tenant, v := confirmedShipment(t)
	ctx := context.Background()
	changeShipment(t, s, tenant, v.ID, "pick", 1)
	changeShipment(t, s, tenant, v.ID, "pack", 2)
	changeShipment(t, s, tenant, v.ID, "dispatch", 3)
	old, found, err := s.Claim(ctx)
	if err != nil || !found {
		t.Fatal("claim", found, err)
	}
	handler := dispatch.Handler{Inventory: h.inventory}
	if err = handler.Handle(ctx, old); err != nil {
		t.Fatal(err)
	}
	// Simulate death after Inventory committed, without saving the checkpoint.
	if _, err = pool.Exec(ctx, `UPDATE fulfillment.shipments SET lease_until='-infinity' WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.Save(ctx, old, false); !errors.Is(err, fulfilldb.ErrLostLease) {
		t.Fatal("expired lease accepted before replacement", err)
	}
	current, found, err := s.Claim(ctx)
	if err != nil || !found || current.Token <= old.Token {
		t.Fatal("reclaim", current, found, err)
	}
	if err = handler.Handle(ctx, current); err != nil {
		t.Fatal("idempotent dispatch", err)
	}
	if err = s.Save(ctx, current, false); err != nil {
		t.Fatal(err)
	}
	if err = s.Save(ctx, old, false); !errors.Is(err, fulfilldb.ErrLostLease) {
		t.Fatal("stale checkpoint accepted", err)
	}
	worker := fr.Worker{Store: s, Handler: handler}
	if did, err := worker.ProcessOne(ctx); err != nil || !did {
		t.Fatal("resume", did, err)
	}
	state, err := s.Get(ctx, tenant, v.ID)
	if err != nil || state.Status != fd.Shipped || state.Dispatched != 2 {
		t.Fatal("not shipped", state, err)
	}
	for sku, want := range map[string]int64{"BOOK": 8, "PEN": 7} {
		stock, err := store.StockAt(ctx, tenant, sku, "east")
		if err != nil || stock.OnHand != want || stock.Allocated != 0 || stock.Reserved != 0 {
			t.Fatal("double physical deduction", stock, err)
		}
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM inventory.outbox WHERE tenant_id=$1 AND event_type='inventory.allocation-shipped.v1'`, tenant).Scan(&n); err != nil || n != 2 {
		t.Fatal("duplicate shipment events", n, err)
	}
	changeShipment(t, s, tenant, v.ID, "deliver", state.Version)
	history, err := s.History(ctx, tenant, v.ID)
	if err != nil || len(history) != 7 {
		t.Fatal("shipment history", len(history), err)
	}
}

type loseShippingResponse struct {
	target inv.Dispatcher
	lost   atomic.Bool
}

func (p *loseShippingResponse) Dispatch(ctx context.Context, tenant, id string) (inv.Hold, error) {
	r, err := p.target.Dispatch(ctx, tenant, id)
	if err == nil && !p.lost.Swap(true) {
		return inv.Hold{}, errors.New("injected response loss after shipping commit")
	}
	return r, err
}
func TestShippingUnknownOutcomeReconcilesWithoutDoubleDeduction(t *testing.T) {
	h, s, tenant, v := confirmedShipment(t)
	ctx := context.Background()
	changeShipment(t, s, tenant, v.ID, "pick", 1)
	changeShipment(t, s, tenant, v.ID, "pack", 2)
	changeShipment(t, s, tenant, v.ID, "dispatch", 3)
	worker := fr.Worker{Store: s, Handler: dispatch.Handler{Inventory: &loseShippingResponse{target: h.inventory}}}
	if _, err := worker.ProcessOne(ctx); err == nil {
		t.Fatal("lost response not injected")
	}
	state, err := s.Get(ctx, tenant, v.ID)
	if err != nil || state.Status != fd.Dispatching || state.Dispatched != 0 || state.Attempts != 1 || state.LastError == "" {
		t.Fatal("unknown outcome not retained", state, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE fulfillment.shipments SET next_attempt_at=now() WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = worker.ProcessOne(ctx); err != nil {
			t.Fatal(err)
		}
	}
	state, err = s.Get(ctx, tenant, v.ID)
	if err != nil || state.Status != fd.Shipped || state.LastError != "" {
		t.Fatal("retry failed", state, err)
	}
	stock, err := store.StockAt(ctx, tenant, "BOOK", "east")
	if err != nil || stock.OnHand != 8 || stock.Version != 4 {
		t.Fatal("unknown outcome duplicated stock effect", stock, err)
	}
}

func TestShippingVersusCompensationHasOneResourceOutcome(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx := context.Background()
	for i := range 8 {
		sku := fmt.Sprintf("BOOK-%d", i)
		receiveAt(t, tenant, sku, "east", sku, 4)
		step := newID(t)
		line := inv.Line{SKU: sku, Quantity: 2}
		if _, err := h.inventory.Reserve(ctx, tenant, step, line, 120); err != nil {
			t.Fatal(err)
		}
		if _, err := h.inventory.Commit(ctx, tenant, step); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(2)
		go func() { defer wg.Done(); _, err := h.inventory.Dispatch(ctx, tenant, step); errs <- err }()
		go func() { defer wg.Done(); errs <- h.inventory.Close(ctx, tenant, step, line) }()
		wg.Wait()
		close(errs)
		wins := 0
		for err := range errs {
			if err == nil {
				wins++
			} else if !errors.Is(err, inv.ErrClosed) {
				t.Fatal("unexpected race outcome", err)
			}
		}
		if wins != 1 {
			t.Fatal("contradictory resource outcomes", wins)
		}
		hold, err := h.inventory.Get(ctx, tenant, step)
		if err != nil {
			t.Fatal(err)
		}
		stock, err := store.StockAt(ctx, tenant, sku, "east")
		if err != nil {
			t.Fatal(err)
		}
		want := int64(4)
		if hold.Status == "shipped" {
			want = 2
		} else if hold.Status != "reversed" {
			t.Fatal("invalid hold", hold)
		}
		if stock.OnHand != want || stock.Allocated != 0 || stock.Reserved != 0 {
			t.Fatal("race corrupted warehouse", stock)
		}
	}
}

func TestFulfillmentFactsIdentityAndTenantIsolation(t *testing.T) {
	_, s, tenant, v := confirmedShipment(t)
	ctx := context.Background()
	for _, query := range []string{
		`UPDATE fulfillment.inbox SET request='{}' WHERE tenant_id=$1`,
		`UPDATE fulfillment.events SET action='changed' WHERE tenant_id=$1`,
		`DELETE FROM fulfillment.allocations WHERE tenant_id=$1`,
		`UPDATE fulfillment.shipments SET state=jsonb_set(state,'{lines}','[]') WHERE tenant_id=$1`,
	} {
		if _, err := pool.Exec(ctx, query, tenant); err == nil {
			t.Fatal("immutable fulfillment fact changed", query)
		}
	}
	other := commerceTenant(t)
	if _, _, err := (changeshipment.Handler{Store: s}).Handle(ctx, other, "cross-tenant", v.ID, "pick", changeshipment.Command{ExpectedVersion: 1}); !errors.Is(err, fd.ErrNotFound) {
		t.Fatal("cross-tenant mutation", err)
	}
	request := api.Request{ID: v.CheckoutID, TenantID: tenant, OrderID: v.OrderID, Allocations: []api.Allocation{{ID: v.Lines[0].ID, SKU: v.Lines[0].SKU, Quantity: 9, WarehouseID: "east"}}}
	if err := s.Submit(ctx, request); !errors.Is(err, fd.ErrConflict) {
		t.Fatal("redelivery changed shipment identity", err)
	}
	state, err := s.Get(ctx, tenant, v.ID)
	if err != nil || state.Version != 1 {
		t.Fatal("failed command changed shipment", state, err)
	}
}

func TestFailedCheckoutDoesNotRequestFulfillment(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx := context.Background()
	product(t, h, tenant, "BOOK", "p", 100, 0)
	receiveAt(t, tenant, "BOOK", "east", "stock", 5)
	v, _ := beginCheckout(t, h, tenant, "test-decline", []catdomain.Line{{SKU: "BOOK", Quantity: 2}})
	pumpCheckout(t, h, tenant, v.ID, "failed")
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ordering.fulfillment_outbox WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 0 {
		t.Fatal("failed order requested fulfillment", n, err)
	}
	stock, err := store.StockAt(ctx, tenant, "BOOK", "east")
	if err != nil || stock.Reserved != 0 || stock.OnHand != 5 {
		t.Fatal("named warehouse not compensated", stock, err)
	}
}

func TestConfirmationAndFulfillmentRequestRollbackTogether(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx := context.Background()
	product(t, h, tenant, "BOOK", "product", 100, 0)
	receiveAt(t, tenant, "BOOK", "east", "stock", 5)
	v, _ := beginCheckout(t, h, tenant, "test-ok", []catdomain.Line{{SKU: "BOOK", Quantity: 2}})
	pumpCheckout(t, h, tenant, v.ID, "confirming")
	hold, err := h.inventory.Get(ctx, tenant, advance.StepID(v.ID, "BOOK"))
	if err != nil {
		t.Fatal(err)
	}
	proof := od.PaymentProof{CheckoutID: v.ID, PaymentID: v.ID, Currency: "USD", AmountMinor: 200, Allocations: []od.Allocation{{ID: hold.ID, SKU: hold.SKU, WarehouseID: hold.WarehouseID, Quantity: hold.Quantity}}}
	r, err := op.NewRequest(tenant, "injected-confirm", "rollback", proof)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = h.orders.Execute(ctx, r, v.OrderID, 2, func(o *od.Order) ([]od.Event, error) {
		events, err := o.ConfirmCheckout(proof)
		if err != nil {
			return nil, err
		}
		return append(events, od.Event{Version: 4, Type: "unsupported-test-fact", SchemaVersion: 1, Data: json.RawMessage(`{}`)}), nil
	})
	if err == nil {
		t.Fatal("invalid batch committed")
	}
	state, err := h.orders.Load(ctx, tenant, v.OrderID)
	if err != nil || state.Status != od.CheckoutPending || state.Version != 2 {
		t.Fatal("partial source confirmation", state, err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ordering.fulfillment_outbox WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 0 {
		t.Fatal("orphan fulfillment request", n, err)
	}
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ordering.fulfillment_outbox WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 1 {
		t.Fatal("recovered request missing", n, err)
	}
}

func TestPoisonWarehouseStreamDoesNotBlockOtherLocations(t *testing.T) {
	tenant := tenant(t)
	ctx := context.Background()
	receiveAt(t, tenant, "BOOK", "east", "east", 2)
	receiveAt(t, tenant, "BOOK", "west", "west", 3)
	if _, err := pool.Exec(ctx, `UPDATE inventory.outbox SET payload='{}' WHERE tenant_id=$1 AND warehouse_id='east'`, tenant); err != nil {
		t.Fatal(err)
	}
	for range 7 {
		_, _ = store.ProcessOne(ctx)
		if _, err := pool.Exec(ctx, `UPDATE inventory.outbox SET available_at=now() WHERE tenant_id=$1 AND warehouse_id='east' AND NOT quarantined`, tenant); err != nil {
			t.Fatal(err)
		}
	}
	view, err := store.ProjectionAt(ctx, tenant, "BOOK", "west")
	if err != nil || view.OnHand != 3 || view.Version != 1 {
		t.Fatal("other warehouse blocked", view, err)
	}
	var quarantined bool
	if err = pool.QueryRow(ctx, `SELECT quarantined FROM inventory.outbox WHERE tenant_id=$1 AND warehouse_id='east'`, tenant).Scan(&quarantined); err != nil || !quarantined {
		t.Fatal("poison warehouse not quarantined", err)
	}
}

func TestConcurrentCheckoutSourcingCannotOversellAcrossWarehouses(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx := context.Background()
	receiveAt(t, tenant, "BOOK", "east", "east", 3)
	receiveAt(t, tenant, "BOOK", "west", "west", 4)
	var accepted, rejected atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for range 24 {
		step := newID(t)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.inventory.Reserve(ctx, tenant, step, inv.Line{SKU: "BOOK", Quantity: 1}, 120)
			if err == nil {
				accepted.Add(1)
			} else if errors.Is(err, inv.ErrUnavailable) {
				rejected.Add(1)
			} else {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if accepted.Load() != 7 || rejected.Load() != 17 {
		t.Fatal("cross-warehouse sourcing oversold or lost capacity", accepted.Load(), rejected.Load())
	}
	for _, wid := range []string{"east", "west"} {
		state, err := store.StockAt(ctx, tenant, "BOOK", wid)
		if err != nil || state.Available() != 0 || state.Reserved != state.OnHand {
			t.Fatal("invalid sourced stock", state, err)
		}
	}
}
