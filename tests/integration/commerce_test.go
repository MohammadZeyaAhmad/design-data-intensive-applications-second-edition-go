//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	inv "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/inventory"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	pay "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	catdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	catdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/publish"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/quote"
	checkdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/advance"
	checkrun "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/runtime"
	invdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	orderdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	orderdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/placeorder"
	paydb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/provider"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const providerTestToken = "integration-provider-only-key-at-least-32-characters"

type commerce struct {
	catalog      *catdb.Store
	orders       *orderdb.Store
	orderService orderdb.Checkout
	jobs         *checkdb.Store
	payments     *paydb.Service
	inventory    invdb.Workflow
	worker       checkrun.Worker
	client       *provider.Client
}

type lostDispatchAck struct{ target orders.Sink }

func (s lostDispatchAck) Submit(ctx context.Context, r orders.Requested) error {
	if err := s.target.Submit(ctx, r); err != nil {
		return err
	}
	return errors.New("injected failure after sink commit")
}

func commerceHarness(t *testing.T) *commerce {
	t.Helper()
	server := httptest.NewServer(provider.Server{Pool: pool, Token: providerTestToken}.Handler())
	t.Cleanup(server.Close)
	client, err := provider.NewClient(server.URL, providerTestToken)
	if err != nil {
		t.Fatal(err)
	}
	h := &commerce{catalog: &catdb.Store{Pool: pool}, orders: orderStore(), jobs: &checkdb.Store{Pool: pool}, payments: &paydb.Service{Pool: pool, Provider: client}, inventory: invdb.Workflow{Store: store}, client: client}
	h.orderService = orderdb.Checkout{Store: h.orders}
	h.worker = checkrun.Worker{Store: h.jobs, Orders: h.orders, Handler: advance.Handler{Inventory: h.inventory, Payments: h.payments, Orders: h.orderService}}
	return h
}
func commerceTenant(t *testing.T) string {
	id := tenant(t)
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM checkout.jobs WHERE tenant_id=$1`, id); err != nil {
			t.Error(err)
		}
		if _, err := pool.Exec(context.Background(), `UPDATE ordering.checkout_outbox SET dispatched_at=now() WHERE tenant_id=$1`, id); err != nil {
			t.Error(err)
		}
	})
	return id
}
func product(t *testing.T, h *commerce, tenant, sku, key string, price, version int64) catdomain.Product {
	t.Helper()
	p, _, err := (publish.Handler{Store: h.catalog}).Handle(context.Background(), tenant, key, catdomain.Product{SKU: sku, Name: "Synthetic product", Currency: "USD", UnitPriceMinor: price, Version: version})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func beginCheckout(t *testing.T, h *commerce, tenant, method string, lines []catdomain.Line) (orders.View, cat.Quote) {
	t.Helper()
	q, _, err := (quote.Handler{Store: h.catalog}).Handle(context.Background(), tenant, "quote", quote.Command{Lines: lines})
	if err != nil {
		t.Fatal(err)
	}
	orderLines := make([]orderdomain.Line, len(lines))
	for i, l := range lines {
		orderLines[i] = orderdomain.Line{SKU: l.SKU, Quantity: l.Quantity}
	}
	s, _, err := (placeorder.Handler{Store: h.orders}).Handle(context.Background(), tenant, "order", placeorder.Command{Lines: orderLines})
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := h.orderService.Begin(context.Background(), tenant, s.ID, "checkout", 1, q, method)
	if err != nil {
		t.Fatal(err)
	}
	if sent, err := h.orders.DispatchOne(context.Background(), h.jobs); err != nil || !sent {
		t.Fatalf("checkout dispatch: %v", err)
	}
	return v, q
}
func pumpCheckout(t *testing.T, h *commerce, tenant, id, until string) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		v, err := h.jobs.Get(context.Background(), tenant, id)
		if err == nil && v.Status == until {
			return
		}
		_, err = h.worker.ProcessOne(context.Background())
		if err != nil && !errors.Is(err, pay.ErrUnknown) {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	v, _ := h.jobs.Get(context.Background(), tenant, id)
	t.Fatalf("checkout did not reach %s: %+v", until, v)
}
func TestCatalogPriceVersionsAndImmutableQuote(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	product(t, h, id, "BOOK", "p1", 1250, 0)
	q, _, err := (quote.Handler{Store: h.catalog}).Handle(context.Background(), id, "quote", quote.Command{Lines: []catdomain.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	product(t, h, id, "BOOK", "p2", 2000, 1)
	stored, err := h.catalog.Quote(context.Background(), id, q.ID)
	if err != nil || stored.TotalMinor != 2500 || stored.Lines[0].PriceVersion != 1 {
		t.Fatal("quote changed after catalog update")
	}
	if _, err = h.catalog.Quote(context.Background(), commerceTenant(t), q.ID); !errors.Is(err, cat.ErrNotFound) {
		t.Fatal("cross-tenant quote exposed")
	}
	var wins, conflicts atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := (publish.Handler{Store: h.catalog}).Handle(context.Background(), id, fmt.Sprintf("race-%d", i), catdomain.Product{SKU: "BOOK", Name: "Synthetic", Currency: "USD", UnitPriceMinor: int64(2001 + i), Version: 2})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, cat.ErrConflict):
				conflicts.Add(1)
			default:
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if wins.Load() != 1 || conflicts.Load() != 9 {
		t.Fatal("price version admitted competing updates")
	}
	if _, err = pool.Exec(context.Background(), `UPDATE catalog.prices SET unit_price_minor=1 WHERE tenant_id=$1`, id); err == nil {
		t.Fatal("historical price was editable")
	}
}
func TestProviderLostResponseReconcilesOneBalancedJournal(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	pid, _ := identity.NewID()
	intent := pay.Intent{TenantID: id, ID: pid, AmountMinor: 2500, Currency: "USD", Method: "test-lost-response"}
	if _, err := h.payments.Capture(context.Background(), intent); !errors.Is(err, pay.ErrUnknown) {
		t.Fatalf("lost response was not uncertain: %v", err)
	}
	r, err := h.payments.Capture(context.Background(), intent)
	if err != nil || r.Status != "captured" {
		t.Fatalf("capture reconciliation failed: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := h.payments.Capture(context.Background(), intent); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	assertJournal(t, id, pid, 1)
	if _, err = pool.Exec(context.Background(), `DELETE FROM payments.journal WHERE tenant_id=$1`, id); err == nil {
		t.Fatal("journal history could be deleted")
	}
	if _, err = h.payments.Close(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	assertJournal(t, id, pid, 2)
}
func assertJournal(t *testing.T, tenant, id string, want int) {
	t.Helper()
	var n int
	var balance int64
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM payments.journal WHERE tenant_id=$1 AND payment_id=$2::uuid`, tenant, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("journal entries=%d want=%d", n, want)
	}
	if err := pool.QueryRow(context.Background(), `SELECT coalesce(sum(leg.amount),0)::bigint FROM payments.journal j CROSS JOIN LATERAL(VALUES(j.amount_minor),(-j.amount_minor)) leg(amount) WHERE tenant_id=$1 AND payment_id=$2::uuid`, tenant, id).Scan(&balance); err != nil || balance != 0 {
		t.Fatalf("unbalanced journal: %d %v", balance, err)
	}
}
func TestClosedResourcesRejectDelayedEffects(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	product(t, h, id, "BOOK", "price", 100, 0)
	receive(t, id, "BOOK", "receive", 10)
	step, _ := identity.NewID()
	line := inv.Line{SKU: "BOOK", Quantity: 2}
	if err := h.inventory.Close(context.Background(), id, step, line); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inventory.Reserve(context.Background(), id, step, line, 120); !errors.Is(err, inv.ErrClosed) {
		t.Fatal("closed step created a delayed hold")
	}
	pid, _ := identity.NewID()
	intent := pay.Intent{TenantID: id, ID: pid, AmountMinor: 100, Currency: "USD", Method: "test-ok"}
	r, err := h.payments.Close(context.Background(), intent)
	if err != nil || r.Status != "voided" {
		t.Fatal("unseen payment was not fenced")
	}
	r, err = h.client.Capture(context.Background(), intent)
	if err != nil || r.Status != "voided" {
		t.Fatal("delayed provider capture charged a closed payment")
	}
	assertJournal(t, id, pid, 0)
}
func TestCheckoutFreezesPriceAndConfirmsOnce(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	product(t, h, id, "BOOK", "price", 1250, 0)
	receive(t, id, "BOOK", "receive", 10)
	v, q := beginCheckout(t, h, id, "test-ok", []catdomain.Line{{SKU: "BOOK", Quantity: 2}})
	product(t, h, id, "BOOK", "new-price", 9000, 1)
	pumpCheckout(t, h, id, v.ID, "confirmed")
	s, err := h.orders.Load(context.Background(), id, v.OrderID)
	if err != nil || s.Status != "confirmed" || s.Pricing.TotalMinor != 2500 {
		t.Fatal("confirmed order lost its accepted price")
	}
	payment, err := h.payments.Get(context.Background(), id, v.ID)
	if err != nil || payment.Status != "captured" || payment.AmountMinor != 2500 {
		t.Fatal("wrong captured amount")
	}
	assertJournal(t, id, v.ID, 1)
	stock, err := store.Stock(context.Background(), id, "BOOK")
	if err != nil || stock.Reserved != 0 || stock.Allocated != 2 {
		t.Fatal("confirmed without allocated inventory")
	}
	replayed, wasReplay, err := h.orderService.Begin(context.Background(), id, v.OrderID, "checkout", 1, q, "test-ok")
	if err != nil || !wasReplay || replayed.ID != v.ID {
		t.Fatal("checkout retry created another attempt")
	}
}
func TestDeclinedCheckoutCompensatesInventory(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	product(t, h, id, "BOOK", "price", 1250, 0)
	receive(t, id, "BOOK", "receive", 10)
	v, _ := beginCheckout(t, h, id, "test-decline", []catdomain.Line{{SKU: "BOOK", Quantity: 2}})
	pumpCheckout(t, h, id, v.ID, "failed")
	s, err := store.Stock(context.Background(), id, "BOOK")
	if err != nil || s.Available() != 10 || s.Reserved != 0 || s.Allocated != 0 {
		t.Fatal("declined checkout leaked stock")
	}
	assertJournal(t, id, v.ID, 0)
	order, err := h.orders.Load(context.Background(), id, v.OrderID)
	if err != nil || order.Status != "checkout_failed" {
		t.Fatal("failed checkout can still confirm")
	}
}
func TestPartialAllocationFailureRefundsAndReverses(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	for _, sku := range []string{"A", "B"} {
		product(t, h, id, sku, "price-"+sku, 1250, 0)
		receive(t, id, sku, "receive-"+sku, 10)
	}
	v, _ := beginCheckout(t, h, id, "test-refund-lost-response", []catdomain.Line{{SKU: "A", Quantity: 2}, {SKU: "B", Quantity: 2}})
	for range 15 {
		view, err := h.jobs.Get(context.Background(), id, v.ID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Status == "allocating" {
			break
		}
		if _, err = h.worker.ProcessOne(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.worker.ProcessOne(context.Background()); err != nil {
		t.Fatal(err)
	} // commits A
	due(t, id, advance.StepID(v.ID, "B"))
	pumpCheckout(t, h, id, v.ID, "failed")
	payment, err := h.payments.Get(context.Background(), id, v.ID)
	if err != nil || payment.Status != "refunded" {
		t.Fatal("failed allocation was not refunded")
	}
	assertJournal(t, id, v.ID, 2)
	for _, sku := range []string{"A", "B"} {
		s, err := store.Stock(context.Background(), id, sku)
		if err != nil || s.Available() != 10 || s.Reserved != 0 || s.Allocated != 0 {
			t.Fatal("partial allocation was not compensated")
		}
	}
}
func TestLostLeaseCannotCheckpointOrRecreateCompensatedHold(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	product(t, h, id, "BOOK", "price", 100, 0)
	receive(t, id, "BOOK", "receive", 5)
	v, _ := beginCheckout(t, h, id, "test-ok", []catdomain.Line{{SKU: "BOOK", Quantity: 2}})
	old, found, err := h.jobs.Claim(context.Background())
	if err != nil || !found {
		t.Fatal(err)
	}
	next, err := h.worker.Handler.Handle(context.Background(), old)
	if err != nil {
		t.Fatal(err)
	} // effect before checkpoint
	if _, err = pool.Exec(context.Background(), `UPDATE checkout.jobs SET lease_until=clock_timestamp()-interval '1 second',cancel_requested=true WHERE tenant_id=$1 AND id=$2::uuid`, id, v.ID); err != nil {
		t.Fatal(err)
	}
	current, found, err := h.jobs.Claim(context.Background())
	if err != nil || !found || current.Token <= old.Token {
		t.Fatal("new lease did not supersede old owner")
	}
	if err = h.jobs.Save(context.Background(), old, next, false); !errors.Is(err, checkdb.ErrLostLease) {
		t.Fatal("stale checkpoint accepted")
	}
	newState, err := h.worker.Handler.Handle(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.jobs.Save(context.Background(), current, newState, false); err != nil {
		t.Fatal(err)
	}
	pumpCheckout(t, h, id, v.ID, "cancelled")
	if _, err = h.inventory.Reserve(context.Background(), id, advance.StepID(v.ID, "BOOK"), inv.Line{SKU: "BOOK", Quantity: 2}, 120); !errors.Is(err, inv.ErrClosed) {
		t.Fatal("stale worker recreated compensated hold")
	}
	s, err := store.Stock(context.Background(), id, "BOOK")
	if err != nil || s.Available() != 5 {
		t.Fatal("cancellation left a reservation")
	}
}

func TestCheckoutDispatchRedeliveryCreatesOneJob(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	product(t, h, id, "BOOK", "price", 100, 0)
	q, _, err := (quote.Handler{Store: h.catalog}).Handle(context.Background(), id, "quote", quote.Command{Lines: []catdomain.Line{{SKU: "BOOK", Quantity: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := (placeorder.Handler{Store: h.orders}).Handle(context.Background(), id, "order", placeorder.Command{Lines: []orderdomain.Line{{SKU: "BOOK", Quantity: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := h.orderService.Begin(context.Background(), id, s.ID, "start", 1, q, "test-ok")
	if err != nil {
		t.Fatal(err)
	}
	if sent, err := h.orders.DispatchOne(context.Background(), lostDispatchAck{h.jobs}); !sent || err == nil {
		t.Fatal("dispatch crash boundary was not exercised")
	}
	if sent, err := h.orders.DispatchOne(context.Background(), h.jobs); err != nil || !sent {
		t.Fatalf("dispatch retry failed: %v", err)
	}
	var count int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, id, v.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate checkout jobs after redelivery")
	}
}

func TestQuoteAndOrderLinesMustAgree(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	product(t, h, id, "BOOK", "price", 100, 0)
	q, _, err := (quote.Handler{Store: h.catalog}).Handle(context.Background(), id, "quote", quote.Command{Lines: []catdomain.Line{{SKU: "BOOK", Quantity: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := (placeorder.Handler{Store: h.orders}).Handle(context.Background(), id, "order", placeorder.Command{Lines: []orderdomain.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = h.orderService.Begin(context.Background(), id, s.ID, "mismatch", 1, q, "test-ok"); !errors.Is(err, orders.ErrInvalid) {
		t.Fatal("quote authorized different quantities")
	}
	got, err := h.orders.Load(context.Background(), id, s.ID)
	if err != nil || got.Version != 1 || got.Status != "placed" {
		t.Fatal("rejected quote froze the order")
	}
	q.Lines[0].Quantity = 2
	q.TotalMinor = 200
	q.ExpiresAt = time.Now().Add(-time.Second)
	if _, _, err = h.orderService.Begin(context.Background(), id, s.ID, "expired", 1, q, "test-ok"); !errors.Is(err, orders.ErrInvalid) {
		t.Fatal("expired quote was accepted")
	}
}

func TestJournalIdentityAndRefundConstraints(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	pid, _ := identity.NewID()
	intent := pay.Intent{TenantID: id, ID: pid, AmountMinor: 2500, Currency: "USD", Method: "test-ok"}
	if _, err := h.payments.Capture(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE payments.intents SET amount_minor=1 WHERE tenant_id=$1 AND id=$2::uuid`, id, pid); err == nil {
		t.Fatal("payment identity could be repurposed")
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO payments.journal(tenant_id,payment_id,kind,currency,amount_minor,debit_account,credit_account) VALUES($1,$2::uuid,'refund','USD',1,'customer_liability','provider_clearing')`, id, pid); err == nil {
		t.Fatal("refund amount did not match capture identity")
	}
	other, _ := identity.NewID()
	intent.ID = other
	if _, err := h.payments.Close(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO payments.journal(tenant_id,payment_id,kind,currency,amount_minor,debit_account,credit_account) VALUES($1,$2::uuid,'refund','USD',2500,'customer_liability','provider_clearing')`, id, other); err == nil {
		t.Fatal("unbacked refund was accepted")
	}
}

func TestRefundReconcilesCaptureMissingFromLocalJournal(t *testing.T) {
	h := commerceHarness(t)
	id := commerceTenant(t)
	pid, _ := identity.NewID()
	intent := pay.Intent{TenantID: id, ID: pid, AmountMinor: 1250, Currency: "USD", Method: "test-ok"}
	if r, err := h.client.Capture(context.Background(), intent); err != nil || r.Status != "captured" {
		t.Fatal("provider capture fixture failed")
	}
	// Crash before the Payments context records the already committed remote effect.
	if _, err := h.payments.Get(context.Background(), id, pid); !errors.Is(err, pay.ErrNotFound) {
		t.Fatal("local journal unexpectedly observed remote capture")
	}
	r, err := h.payments.Close(context.Background(), intent)
	if err != nil || r.Status != "refunded" {
		t.Fatalf("unknown capture did not reconcile during refund: %v", err)
	}
	assertJournal(t, id, pid, 2)
}
