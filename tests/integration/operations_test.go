//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/operations"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	catdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/publish"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/quote"
	checkdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/advance"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/features/changeshipment"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/features/dispatch"
	fr "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/runtime"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/receivestock"
	od "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/placeorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/operator"
)

func requestContext(t *testing.T, tenant string) (context.Context, envelope.Metadata) {
	t.Helper()
	request := newID(t)
	m := envelope.Metadata{Version: 1, Actor: "tenant-key:" + tenant, CorrelationID: newID(t), CausationID: request, OriginRequestID: request}
	return envelope.With(context.Background(), m), m
}
func operatorContext(t *testing.T, tenant string) context.Context {
	t.Helper()
	ctx, err := operator.Local(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}
func control(t *testing.T, target api.Workflow, ctx context.Context, tenant, id, action, key string, version int64) api.Result {
	t.Helper()
	result, _, err := target.Control(ctx, api.Command{TenantID: tenant, ID: id, Action: action, Reason: "investigation", Key: key, ExpectedVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func pendingCheckout(t *testing.T) (*commerce, string, orders.View) {
	t.Helper()
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	product(t, h, tenant, "BOOK", "p", 100, 0)
	receive(t, tenant, "BOOK", "stock", 10)
	v, _ := beginCheckout(t, h, tenant, "test-ok", []catdomain.Line{{SKU: "BOOK", Quantity: 2}})
	return h, tenant, v
}
func assertLineage(t *testing.T, m envelope.Metadata, root envelope.Metadata, actor string) {
	t.Helper()
	if !m.Valid() || m.Version != 1 || m.CorrelationID != root.CorrelationID || m.OriginRequestID != root.OriginRequestID || m.Actor != actor || m.CausationID == "" {
		t.Fatal("lineage changed", m, root, actor)
	}
}

func TestProvenanceAcrossCheckoutProviderAndShipment(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx, root := requestContext(t, tenant)
	if _, _, err := (publish.Handler{Store: h.catalog}).Handle(ctx, tenant, "p", catdomain.Product{SKU: "BOOK", Name: "Book", Currency: "USD", UnitPriceMinor: 100}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (receivestock.Handler{Store: store}).Handle(ctx, tenant, "stock", receivestock.Command{SKU: "BOOK", WarehouseID: "east", Quantity: 10}); err != nil {
		t.Fatal(err)
	}
	q, _, err := (quote.Handler{Store: h.catalog}).Handle(ctx, tenant, "quote", quote.Command{Lines: []catdomain.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	order, _, err := (placeorder.Handler{Store: h.orders}).Handle(ctx, tenant, "order", placeorder.Command{Lines: []od.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := h.orderService.Begin(ctx, tenant, order.ID, "checkout", 1, q, "test-lost-response")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.orders.DispatchOne(context.Background(), h.jobs); err != nil {
		t.Fatal(err)
	}
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
	page, err := h.orders.Events(ctx, tenant, order.ID, 0, 100)
	if err != nil || len(page.Events) != 3 {
		t.Fatal("history", page, err)
	}
	assertLineage(t, page.Events[0].Metadata, root, "tenant-key:"+tenant)
	assertLineage(t, page.Events[1].Metadata, root, "tenant-key:"+tenant)
	assertLineage(t, page.Events[2].Metadata, root, "worker:checkout")
	var request orders.Requested
	if err = pool.QueryRow(ctx, `SELECT request FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID).Scan(&request); err != nil {
		t.Fatal(err)
	}
	if request.Metadata.CausationID != page.Events[1].ID {
		t.Fatal("dispatch lost source event cause", request.Metadata)
	}
	for _, item := range []struct{ query, actor string }{
		{`SELECT metadata FROM catalog.prices WHERE tenant_id=$1`, "tenant-key:" + tenant},
		{`SELECT metadata FROM catalog.quotes WHERE tenant_id=$1`, "tenant-key:" + tenant},
		{`SELECT metadata FROM payments.journal WHERE tenant_id=$1`, "worker:checkout"},
		{`SELECT metadata FROM provider.operations WHERE tenant_id=$1`, "worker:payment-provider"},
		{`SELECT metadata FROM inventory.outbox WHERE tenant_id=$1 AND event_type='inventory.reservation-committed.v1'`, "worker:checkout"},
	} {
		var m envelope.Metadata
		if err = pool.QueryRow(ctx, item.query, tenant).Scan(&m); err != nil {
			t.Fatal(err)
		}
		assertLineage(t, m, root, item.actor)
	}
	s := fulfillStore()
	if _, err = h.orders.DispatchFulfillmentOne(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	views, err := s.ForOrder(ctx, tenant, order.ID)
	if err != nil || len(views) != 1 {
		t.Fatal("shipment", views, err)
	}
	shipment := views[0]
	if shipment.Metadata.CausationID != page.Events[2].ID {
		t.Fatal("fulfillment lost confirmation cause", shipment.Metadata)
	}
	for i, action := range []string{"pick", "pack", "dispatch"} {
		c := changeshipment.Command{ExpectedVersion: int64(i + 1)}
		if action == "dispatch" {
			c.TrackingReference = "TEST-TRACE"
		}
		if _, _, err = (changeshipment.Handler{Store: s}).Handle(ctx, tenant, action, shipment.ID, action, c); err != nil {
			t.Fatal(err)
		}
	}
	w := fr.Worker{Store: s, Handler: dispatch.Handler{Inventory: h.inventory}}
	if _, err = w.ProcessOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	history, err := s.History(ctx, tenant, shipment.ID)
	if err != nil || len(history) != 5 {
		t.Fatal("shipment history", history, err)
	}
	assertLineage(t, history[0].Metadata, root, "worker:fulfillment")
	assertLineage(t, history[3].Metadata, root, "tenant-key:"+tenant)
	assertLineage(t, history[4].Metadata, root, "worker:fulfillment")
	var shipped envelope.Metadata
	if err = pool.QueryRow(ctx, `SELECT metadata FROM inventory.outbox WHERE tenant_id=$1 AND event_type='inventory.allocation-shipped.v1'`, tenant).Scan(&shipped); err != nil {
		t.Fatal(err)
	}
	assertLineage(t, shipped, root, "worker:fulfillment")
}

func TestIdempotentOrderRetryPreservesAcceptedProvenance(t *testing.T) {
	tenant := commerceTenant(t)
	h := commerceHarness(t)
	ctx, first := requestContext(t, tenant)
	other, _ := requestContext(t, tenant)
	c := placeorder.Command{Lines: []od.Line{{SKU: "BOOK", Quantity: 2}}}
	state, _, err := (placeorder.Handler{Store: h.orders}).Handle(ctx, tenant, "order", c)
	if err != nil {
		t.Fatal(err)
	}
	again, replayed, err := (placeorder.Handler{Store: h.orders}).Handle(other, tenant, "order", c)
	if err != nil || !replayed || !reflect.DeepEqual(state, again) {
		t.Fatal("replay changed", again, replayed, err)
	}
	page, err := h.orders.Events(ctx, tenant, state.ID, 0, 100)
	if err != nil || len(page.Events) != 1 {
		t.Fatal(err)
	}
	assertLineage(t, page.Events[0].Metadata, first, "tenant-key:"+tenant)
	full, err := h.orders.Replay(other, tenant, state.ID)
	if err != nil || !reflect.DeepEqual(state, full) {
		t.Fatal("metadata changed reducer", full, err)
	}
}

func TestLegacyEnvelopeRemainsUnknownAndMalformedEnvelopeRollsBack(t *testing.T) {
	h := commerceHarness(t)
	tenant := commerceTenant(t)
	ctx := context.Background()
	id := newID(t)
	if _, err := pool.Exec(ctx, `INSERT INTO ordering.streams(tenant_id,id,version) VALUES($1,$2::uuid,1)`, tenant, id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO ordering.events(tenant_id,order_id,version,command_id,event_type,schema_version,payload) VALUES($1,$2::uuid,1,gen_random_uuid(),'order.placed',2,'{"lines":[{"sku":"BOOK","quantity":2}]}')`, tenant, id); err != nil {
		t.Fatal(err)
	}
	page, err := h.orders.Events(ctx, tenant, id, 0, 10)
	if err != nil || len(page.Events) != 1 || page.Events[0].Metadata != (envelope.Metadata{}) {
		t.Fatal("historical identity invented", page, err)
	}
	if _, err = h.orders.Load(ctx, tenant, id); err != nil {
		t.Fatal(err)
	}
	invalid := envelope.With(ctx, envelope.Metadata{Version: 0, Actor: "operator:local"})
	if _, _, err = (placeorder.Handler{Store: h.orders}).Handle(invalid, tenant, "invalid", placeorder.Command{Lines: []od.Line{{SKU: "BOOK", Quantity: 1}}}); !errors.Is(err, envelope.ErrInvalid) {
		t.Fatal("malformed envelope accepted", err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ordering.command_results WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 0 {
		t.Fatal("invalid provenance left command", n, err)
	}
	for _, raw := range []string{`{"version":0,"actor":"operator:local"}`, `{"version":1}`, `{"version":0,"unexpected":true}`} {
		var valid bool
		if err = pool.QueryRow(ctx, `SELECT platform.valid_metadata($1::jsonb)`, raw).Scan(&valid); err != nil || valid {
			t.Fatal("SQL accepted invalid envelope", raw, err)
		}
	}
}

func TestPausedCheckoutResumesWithoutResettingIdentity(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	ctx := operatorContext(t, tenant)
	var before []byte
	if err := pool.QueryRow(ctx, `SELECT state FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	paused := control(t, h.jobs, ctx, tenant, v.ID, "pause", "pause", 0)
	if !paused.View.Paused || paused.View.ControlVersion != 1 {
		t.Fatal("pause result", paused)
	}
	if did, err := h.worker.ProcessOne(context.Background()); err != nil || did {
		t.Fatal("paused job claimed", did, err)
	}
	var after []byte
	if err := pool.QueryRow(ctx, `SELECT state FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID).Scan(&after); err != nil || string(before) != string(after) {
		t.Fatal("pause rewrote business progress", err)
	}
	control(t, h.jobs, ctx, tenant, v.ID, "resume", "resume", 1)
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
	again, replayed, err := h.jobs.Control(ctx, api.Command{TenantID: tenant, ID: v.ID, Action: "pause", Reason: "investigation", ExpectedVersion: 0, Key: "pause"})
	if err != nil || !replayed || again.AuditID != paused.AuditID || again.View.ControlVersion != 1 {
		t.Fatal("accepted control replay changed", again, replayed, err)
	}
	if _, _, err = h.jobs.Control(ctx, api.Command{TenantID: tenant, ID: v.ID, Action: "retry", Reason: "operator_retry", ExpectedVersion: 2, Key: "terminal"}); !errors.Is(err, api.ErrTerminal) {
		t.Fatal("terminal retry accepted", err)
	}
	page, err := h.jobs.ControlHistory(ctx, tenant, v.ID, 0, 100)
	if err != nil || len(page.Entries) != 2 {
		t.Fatal("audit history", page, err)
	}
}

func TestControlRejectsActiveLeaseAndFencesExpiredCheckpoint(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	ctx := operatorContext(t, tenant)
	old, found, err := h.jobs.Claim(context.Background())
	if err != nil || !found {
		t.Fatal(err)
	}
	c := api.Command{TenantID: tenant, ID: v.ID, Action: "pause", Reason: "investigation", ExpectedVersion: 0, Key: "pause"}
	if _, _, err = h.jobs.Control(ctx, c); !errors.Is(err, api.ErrBusy) {
		t.Fatal("active lease stolen", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE checkout.jobs SET lease_until='-infinity' WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = h.jobs.Control(ctx, c); err != nil {
		t.Fatal("rejected request consumed identity", err)
	}
	// An expired old call may still complete externally while paused. Its token
	// cannot checkpoint; resumption reuses the same resource intent.
	next, err := h.worker.Handler.Handle(context.Background(), old)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.jobs.Save(context.Background(), old, next, false); !errors.Is(err, checkdb.ErrLostLease) {
		t.Fatal("old checkpoint accepted", err)
	}
	if did, err := h.worker.ProcessOne(context.Background()); err != nil || did {
		t.Fatal("pause lost", did, err)
	}
	control(t, h.jobs, ctx, tenant, v.ID, "resume", "resume", 1)
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM inventory.outbox WHERE tenant_id=$1 AND event_type='inventory.stock-reserved.v1'`, tenant).Scan(&n); err != nil || n != 1 {
		t.Fatal("old call duplicated stock intent", n, err)
	}
}

func TestConcurrentOperatorsUseControlVersionsAndImmutableAudit(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	ctx := operatorContext(t, tenant)
	var won, conflicts atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := h.jobs.Control(ctx, api.Command{TenantID: tenant, ID: v.ID, Action: "pause", Reason: "investigation", Key: fmt.Sprintf("pause-%d", i), ExpectedVersion: 0})
			if err == nil {
				won.Add(1)
			} else if errors.Is(err, api.ErrConflict) {
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
	control(t, h.jobs, ctx, tenant, v.ID, "resume", "resume", 1)
	first, err := h.jobs.ControlHistory(ctx, tenant, v.ID, 0, 1)
	if err != nil || len(first.Entries) != 1 || first.NextAfterVersion != 1 {
		t.Fatal("first audit page", first, err)
	}
	second, err := h.jobs.ControlHistory(ctx, tenant, v.ID, first.NextAfterVersion, 1)
	if err != nil || len(second.Entries) != 1 || second.NextAfterVersion != 2 {
		t.Fatal("second audit page", second, err)
	}
	if first.Entries[0].Metadata.Actor != "operator:local" {
		t.Fatal("missing operator audit")
	}
	if _, err = pool.Exec(ctx, `UPDATE checkout.control_audit SET reason='operator_retry' WHERE tenant_id=$1`, tenant); err == nil {
		t.Fatal("audit rewritten")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM checkout.control_audit WHERE tenant_id=$1`, tenant); err == nil {
		t.Fatal("audit deleted")
	}
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
}

func TestConcurrentControlRetriesHaveOneAuditReceipt(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	ctx := operatorContext(t, tenant)
	var wg sync.WaitGroup
	var fresh atomic.Int64
	receipts := make(chan string, 20)
	errs := make(chan error, 20)
	c := api.Command{TenantID: tenant, ID: v.ID, Action: "pause", Reason: "investigation", Key: "same-key", ExpectedVersion: 0}
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, replayed, err := h.jobs.Control(ctx, c)
			if err != nil {
				errs <- err
				return
			}
			if !replayed {
				fresh.Add(1)
			}
			receipts <- out.AuditID
		}()
	}
	wg.Wait()
	close(errs)
	close(receipts)
	for err := range errs {
		t.Fatal(err)
	}
	var first string
	for id := range receipts {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("multiple audit identities")
		}
	}
	if fresh.Load() != 1 {
		t.Fatal("multiple fresh controls", fresh.Load())
	}
	c.Reason = "operator_retry"
	if _, _, err := h.jobs.Control(ctx, c); !errors.Is(err, commands.ErrConflict) {
		t.Fatal("changed command replay accepted", err)
	}
	control(t, h.jobs, ctx, tenant, v.ID, "resume", "resume", 1)
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
}

func TestAuditWriteFailureRollsBackSchedulingAndRequest(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	ctx := operatorContext(t, tenant)
	if _, err := pool.Exec(ctx, `CREATE FUNCTION checkout.test_reject_control() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$; CREATE TRIGGER test_reject_control BEFORE INSERT ON checkout.control_audit FOR EACH ROW EXECUTE FUNCTION checkout.test_reject_control()`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := pool.Exec(ctx, `DROP TRIGGER IF EXISTS test_reject_control ON checkout.control_audit; DROP FUNCTION IF EXISTS checkout.test_reject_control()`); err != nil {
			t.Error(err)
		}
	}()
	c := api.Command{TenantID: tenant, ID: v.ID, Action: "pause", Reason: "investigation", Key: "rollback", ExpectedVersion: 0}
	if _, _, err := h.jobs.Control(ctx, c); err == nil {
		t.Fatal("audit failure ignored")
	}
	view, err := h.jobs.Inspect(ctx, tenant, v.ID)
	if err != nil || view.Paused || view.ControlVersion != 0 || view.LeaseToken != 0 {
		t.Fatal("partial scheduling mutation", view, err)
	}
	if _, err = pool.Exec(ctx, `DROP TRIGGER test_reject_control ON checkout.control_audit; DROP FUNCTION checkout.test_reject_control()`); err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := h.jobs.Control(ctx, c); err != nil || replayed {
		t.Fatal("failed audit consumed key", replayed, err)
	}
	control(t, h.jobs, ctx, tenant, v.ID, "resume", "resume", 1)
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
}

func TestShipmentRecoveryControlsPreserveUnknownHandoff(t *testing.T) {
	h, s, tenant, v := confirmedShipment(t)
	ctx := operatorContext(t, tenant)
	changeShipment(t, s, tenant, v.ID, "pick", 1)
	changeShipment(t, s, tenant, v.ID, "pack", 2)
	changeShipment(t, s, tenant, v.ID, "dispatch", 3)
	w := fr.Worker{Store: s, Handler: dispatch.Handler{Inventory: &loseShippingResponse{target: h.inventory}}}
	if _, err := w.ProcessOne(context.Background()); err == nil {
		t.Fatal("lost response not injected")
	}
	control(t, s, ctx, tenant, v.ID, "pause", "pause", 0)
	if did, err := w.ProcessOne(context.Background()); err != nil || did {
		t.Fatal("paused shipment claimed", did, err)
	}
	if _, _, err := s.Control(ctx, api.Command{TenantID: tenant, ID: v.ID, Action: "retry", Reason: "operator_retry", ExpectedVersion: 1, Key: "paused-retry"}); !errors.Is(err, api.ErrConflict) {
		t.Fatal("retry resumed paused work", err)
	}
	control(t, s, ctx, tenant, v.ID, "resume", "resume", 1)
	if _, err := pool.Exec(ctx, `UPDATE fulfillment.shipments SET next_attempt_at=now()+interval '1 day' WHERE tenant_id=$1 AND id=$2::uuid`, tenant, v.ID); err != nil {
		t.Fatal(err)
	}
	control(t, s, ctx, tenant, v.ID, "retry", "retry", 2)
	for range 2 {
		if _, err := w.ProcessOne(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	stock, err := store.StockAt(context.Background(), tenant, "BOOK", "east")
	if err != nil || stock.OnHand != 8 || stock.Version != 4 {
		t.Fatal("recovery duplicated physical effect", stock, err)
	}
	history, err := s.ControlHistory(ctx, tenant, v.ID, 0, 100)
	if err != nil || len(history.Entries) != 3 {
		t.Fatal("shipment control audit", history, err)
	}
}

func TestDiagnosticActorCannotForgeOperatorScope(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	ctx, m := requestContext(t, tenant)
	m.Actor = "operator:local"
	ctx = envelope.With(ctx, m)
	c := api.Command{TenantID: tenant, ID: v.ID, Action: "pause", Reason: "investigation", ExpectedVersion: 0, Key: "forged"}
	if _, _, err := h.jobs.Control(ctx, c); !errors.Is(err, api.ErrForbidden) {
		t.Fatal("diagnostic actor granted authority", err)
	}
	other := commerceTenant(t)
	scope := operatorContext(t, other)
	if _, _, err := h.jobs.Control(scope, c); !errors.Is(err, api.ErrForbidden) {
		t.Fatal("operator crossed granted scope", err)
	}
	if _, err := h.jobs.Inspect(scope, other, v.ID); !errors.Is(err, api.ErrNotFound) {
		t.Fatal("cross-tenant workflow exposed", err)
	}
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
}

func TestPausedCancellationRetainsFirstRequestProvenance(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	op := operatorContext(t, tenant)
	control(t, h.jobs, op, tenant, v.ID, "pause", "pause", 0)
	first, m := requestContext(t, tenant)
	second, _ := requestContext(t, tenant)
	if err := h.jobs.Cancel(first, tenant, v.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.jobs.Cancel(second, tenant, v.ID); err != nil {
		t.Fatal(err)
	}
	view, err := h.jobs.Get(context.Background(), tenant, v.ID)
	if err != nil || !view.Paused || !view.CancelRequested {
		t.Fatal("cancel changed scheduling", view, err)
	}
	assertLineage(t, view.CancellationMetadata, m, "tenant-key:"+tenant)
	control(t, h.jobs, op, tenant, v.ID, "resume", "resume", 1)
	pumpCheckout(t, h, tenant, v.ID, "cancelled")
	page, err := h.orders.Events(context.Background(), tenant, v.OrderID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	assertLineage(t, page.Events[len(page.Events)-1].Metadata, m, "worker:checkout")
}

func TestOriginMetadataAndDispatchProvenanceAreImmutable(t *testing.T) {
	h, s, tenant, v := confirmedShipment(t)
	ctx := context.Background()
	changeShipment(t, s, tenant, v.ID, "pick", 1)
	changeShipment(t, s, tenant, v.ID, "pack", 2)
	changeShipment(t, s, tenant, v.ID, "dispatch", 3)
	for _, query := range []string{
		`UPDATE checkout.jobs SET metadata='{"version":1}' WHERE tenant_id=$1`,
		`UPDATE payments.intents SET metadata='{"version":0}' WHERE tenant_id=$1`,
		`UPDATE provider.operations SET metadata='{"version":0}' WHERE tenant_id=$1`,
		`UPDATE inventory.reservations SET metadata='{"version":0}' WHERE tenant_id=$1`,
		`UPDATE fulfillment.shipments SET dispatch_metadata='{"version":0}' WHERE tenant_id=$1`,
	} {
		if _, err := pool.Exec(ctx, query, tenant); err == nil {
			t.Fatal("origin metadata changed", query)
		}
	}
	w := fr.Worker{Store: s, Handler: dispatch.Handler{Inventory: h.inventory}}
	for range 2 {
		if _, err := w.ProcessOne(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPausedCheckoutHoldsStillExpireAndResumeCompensates(t *testing.T) {
	h, tenant, v := pendingCheckout(t)
	ctx := context.Background()
	op := operatorContext(t, tenant)
	pumpCheckout(t, h, tenant, v.ID, "capturing")
	control(t, h.jobs, op, tenant, v.ID, "pause", "pause", 0)
	step := advance.StepID(v.ID, "BOOK")
	var original envelope.Metadata
	if err := pool.QueryRow(ctx, `SELECT metadata FROM inventory.reservations WHERE tenant_id=$1 AND id=$2::uuid`, tenant, step).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE inventory.reservations SET expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2::uuid`, tenant, step); err != nil {
		t.Fatal(err)
	}
	if did, err := store.ExpireOne(ctx); err != nil || !did {
		t.Fatal("paused checkout suspended expiry", did, err)
	}
	var expired envelope.Metadata
	if err := pool.QueryRow(ctx, `SELECT metadata FROM inventory.outbox WHERE tenant_id=$1 AND event_type='inventory.reservation-expired.v1'`, tenant).Scan(&expired); err != nil {
		t.Fatal(err)
	}
	assertLineage(t, expired, original, "worker:inventory-expiry")
	control(t, h.jobs, op, tenant, v.ID, "resume", "resume", 1)
	pumpCheckout(t, h, tenant, v.ID, "failed")
	stock, err := store.Stock(ctx, tenant, "BOOK")
	if err != nil || stock.Reserved != 0 || stock.Allocated != 0 || stock.OnHand != 10 {
		t.Fatal("expired hold recovery changed stock", stock, err)
	}
	receipt, err := h.payments.Get(ctx, tenant, v.ID)
	if err != nil || receipt.Status != "refunded" {
		t.Fatal("resumed expiry failed to compensate capture", receipt, err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM payments.journal WHERE tenant_id=$1 AND payment_id=$2::uuid`, tenant, v.ID).Scan(&n); err != nil || n != 2 {
		t.Fatal("compensation journal", n, err)
	}
}
