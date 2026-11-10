//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	fulfill "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/fulfillment"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	catdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/quote"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/reservestock"
	od "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/amendorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/placeorder"
	inboxdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/messaging/postgres"
)

type capturedMessage struct {
	topic   string
	message api.Message
}
type capturePublisher struct {
	messages []capturedMessage
	lose     bool
}

func (p *capturePublisher) Publish(_ context.Context, topic string, m api.Message) error {
	p.messages = append(p.messages, capturedMessage{topic, m})
	if p.lose {
		p.lose = false
		return errors.New("injected acknowledgment loss")
	}
	return nil
}
func brokerTenant(t *testing.T) string {
	t.Helper()
	tenant := commerceTenant(t)
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM messaging.inbox WHERE tenant_id=$1`, tenant); err != nil {
			t.Error(err)
		}
	})
	return tenant
}
func inbox() inboxdb.Inbox { return inboxdb.Inbox{Pool: pool, Group: api.Group} }
func stage(t *testing.T, c capturedMessage, offset int64) {
	t.Helper()
	b, err := api.Encode(c.topic, c.message)
	if err != nil {
		t.Fatal(err)
	}
	if err = inbox().Stage(context.Background(), api.Record{Topic: c.topic, Partition: api.Partition(c.message.Key()), Offset: offset, Key: c.message.Key(), Value: b}); err != nil {
		t.Fatal(err)
	}
}
func offset(t *testing.T) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), `SELECT coalesce(max(offset_id),-1)+1 FROM messaging.deliveries`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func drainInbox(t *testing.T, h api.Handler) {
	t.Helper()
	for range 500 {
		did, err := inbox().ProcessOne(context.Background(), h)
		if err != nil {
			t.Fatal(err)
		}
		if !did {
			return
		}
	}
	t.Fatal("inbox did not drain")
}
func inventoryHandler() api.Handler {
	return api.HandlerFunc(func(ctx context.Context, topic string, m api.Message) error {
		if topic != api.InventoryTopic {
			return api.ErrInvalid
		}
		return store.ApplyMessage(ctx, m)
	})
}
func publishInventory(t *testing.T, p api.Publisher) bool {
	t.Helper()
	did, err := store.PublishOne(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return did
}
func capturedStock(t *testing.T, tenant, sku string, q int64) capturedMessage {
	t.Helper()
	receive(t, tenant, sku, "receive-"+sku, q)
	p := &capturePublisher{}
	if !publishInventory(t, p) || len(p.messages) != 1 {
		t.Fatal("missing publication")
	}
	return p.messages[0]
}

func TestBrokerAckLossRetriesTheSameUnpublishedPrefix(t *testing.T) {
	tenant := brokerTenant(t)
	ctx := context.Background()
	receive(t, tenant, "BOOK", "stock", 3)
	if _, _, err := (reservestock.Handler{Store: store}).Handle(ctx, tenant, "reserve", reservestock.Command{SKU: "BOOK", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	p := &capturePublisher{lose: true}
	if did, err := store.PublishOne(ctx, p); !did || err == nil {
		t.Fatal("ack loss not injected")
	}
	if !publishInventory(t, p) || !publishInventory(t, p) || len(p.messages) != 3 {
		t.Fatal("missing prefix retry")
	}
	if !reflect.DeepEqual(p.messages[0], p.messages[1]) || p.messages[1].message.Sequence != 1 || p.messages[2].message.Sequence != 2 {
		t.Fatal("publication overtook uncertain prefix")
	}
	start := offset(t)
	for i, m := range p.messages {
		stage(t, m, start+int64(i))
	}
	drainInbox(t, inventoryHandler())
	view, err := store.Projection(ctx, tenant, "BOOK")
	if err != nil || view.Version != 2 || view.Reserved != 1 {
		t.Fatal("duplicate publication changed projection", view, err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM inventory.projection_receipts WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 2 {
		t.Fatal("duplicate owner effect", n, err)
	}
}

func TestInboxRedeliveryAfterOwnerCommitHasOneEffect(t *testing.T) {
	tenant := brokerTenant(t)
	m := capturedStock(t, tenant, "BOOK", 4)
	stage(t, m, offset(t))
	lost := true
	h := api.HandlerFunc(func(ctx context.Context, topic string, m api.Message) error {
		if err := store.ApplyMessage(ctx, m); err != nil {
			return err
		}
		if lost {
			lost = false
			return errors.New("injected failure after owner commit")
		}
		return nil
	})
	if did, err := inbox().ProcessOne(context.Background(), h); !did || err == nil {
		t.Fatal("owner commit boundary not injected")
	}
	if _, err := pool.Exec(context.Background(), `UPDATE messaging.inbox SET available_at=now() WHERE tenant_id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	drainInbox(t, h)
	view, err := store.Projection(context.Background(), tenant, "BOOK")
	if err != nil || view.Version != 1 || view.OnHand != 4 {
		t.Fatal("owner effect repeated", view, err)
	}
	var n int
	if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM inventory.projection_receipts WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 1 {
		t.Fatal("owner inbox not atomic", n, err)
	}
}

func TestInvalidWireIsDurablyRejectedBeforeBrokerAcknowledgment(t *testing.T) {
	tenant := brokerTenant(t)
	m := capturedStock(t, tenant, "BOOK", 2)
	ctx := context.Background()
	start := offset(t)
	bad := api.Record{Topic: m.topic, Partition: api.Partition(m.message.Key()), Offset: start, Key: m.message.Key(), Value: []byte(`{"version":99}`)}
	if err := inbox().Stage(ctx, bad); err != nil {
		t.Fatal(err)
	}
	b, _ := api.Encode(m.topic, m.message)
	bad.Offset++
	bad.Value = b
	bad.Partition = (bad.Partition + 1) % api.Partitions
	if err := inbox().Stage(ctx, bad); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messaging.rejected WHERE offset_id IN($1,$2)`, start, start+1).Scan(&n); err != nil || n != 2 {
		t.Fatal("wire rejection not durable", n, err)
	}
	if _, err := store.Projection(ctx, tenant, "BOOK"); err == nil {
		t.Fatal("invalid wire applied")
	}
}

func TestConflictingMessageIdentityQuarantinesWithoutPayloadRewrite(t *testing.T) {
	tenant := brokerTenant(t)
	m := capturedStock(t, tenant, "BOOK", 2)
	start := offset(t)
	stage(t, m, start)
	changed := m
	changed.message.Data = json.RawMessage(`{"stock":{"tenant_id":"wrong"}}`)
	stage(t, changed, start+1)
	var status string
	var body []byte
	if err := pool.QueryRow(context.Background(), `SELECT status,envelope FROM messaging.inbox WHERE tenant_id=$1`, tenant).Scan(&status, &body); err != nil || status != "quarantined" {
		t.Fatal("identity conflict hidden", status, err)
	}
	original, _ := api.Encode(m.topic, m.message)
	if !api.Same(body, original) {
		t.Fatal("conflict rewrote accepted payload")
	}
	ctx := operatorContext(t, tenant)
	if _, _, err := inbox().Retry(ctx, inboxdb.Retry{TenantID: tenant, Topic: m.topic, ID: m.message.ID, Key: "retry", Reason: "consumer_upgraded", ExpectedVersion: 0}); err != nil {
		t.Fatal(err)
	}
	drainInbox(t, inventoryHandler())
	if _, err := pool.Exec(ctx, `UPDATE messaging.inbox SET envelope='{}' WHERE tenant_id=$1`, tenant); err == nil {
		t.Fatal("accepted wire payload changed")
	}
}

func TestPoisonMessageBlocksOnlyItsStreamOnTheSamePartition(t *testing.T) {
	tenant := brokerTenant(t)
	first := capturedStock(t, tenant, "BOOK", 2)
	other := ""
	for i := range 100 {
		sku := fmt.Sprintf("OTHER-%d", i)
		m := first.message
		m.StreamID = api.InventoryStream(sku, "")
		if api.Partition(m.Key()) == api.Partition(first.message.Key()) {
			other = sku
			break
		}
	}
	if other == "" {
		t.Fatal("no colliding partition fixture")
	}
	receive(t, tenant, "BOOK", "second", 1)
	p := &capturePublisher{}
	publishInventory(t, p)
	second := p.messages[0]
	unrelated := capturedStock(t, tenant, other, 5)
	start := offset(t)
	stage(t, first, start)
	stage(t, second, start+1)
	stage(t, unrelated, start+2)
	unsupported := true
	handler := api.HandlerFunc(func(ctx context.Context, topic string, m api.Message) error {
		if unsupported && m.ID == first.message.ID {
			return errors.New("consumer schema unavailable")
		}
		return store.ApplyMessage(ctx, m)
	})
	for range 8 {
		_, _ = inbox().ProcessOne(context.Background(), handler)
		if _, err := pool.Exec(context.Background(), `UPDATE messaging.inbox SET available_at=now() WHERE tenant_id=$1`, tenant); err != nil {
			t.Fatal(err)
		}
	}
	if view, err := store.Projection(context.Background(), tenant, other); err != nil || view.OnHand != 5 {
		t.Fatal("same partition's other stream blocked", view, err)
	}
	if _, err := store.Projection(context.Background(), tenant, "BOOK"); err == nil {
		t.Fatal("poison stream skipped head")
	}
	unsupported = false
	ctx := operatorContext(t, tenant)
	if _, _, err := inbox().Retry(ctx, inboxdb.Retry{TenantID: tenant, Topic: first.topic, ID: first.message.ID, Key: "repair", Reason: "consumer_upgraded", ExpectedVersion: 0}); err != nil {
		t.Fatal(err)
	}
	drainInbox(t, handler)
	if view, err := store.Projection(ctx, tenant, "BOOK"); err != nil || view.OnHand != 3 || view.Version != 2 {
		t.Fatal("stream failed ordered recovery", view, err)
	}
}

func TestLockedInboxHeadCannotBeOvertaken(t *testing.T) {
	tenant := brokerTenant(t)
	a := capturedStock(t, tenant, "BOOK", 2)
	receive(t, tenant, "BOOK", "second", 1)
	p := &capturePublisher{}
	publishInventory(t, p)
	b := p.messages[0]
	other := capturedStock(t, tenant, "PEN", 4)
	start := offset(t)
	stage(t, a, start)
	stage(t, b, start+1)
	stage(t, other, start+2)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(context.Background(), `SELECT 1 FROM messaging.inbox WHERE message_id=$1::uuid FOR UPDATE`, a.message.ID); err != nil {
		t.Fatal(err)
	}
	if did, err := inbox().ProcessOne(context.Background(), inventoryHandler()); err != nil || !did {
		t.Fatal("independent stream did not progress", did, err)
	}
	if _, err = store.Projection(context.Background(), tenant, "BOOK"); err == nil {
		t.Fatal("locked head overtaken")
	}
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	drainInbox(t, inventoryHandler())
}

func TestTransportCannotOverrideAuthoritativeSource(t *testing.T) {
	tenant := brokerTenant(t)
	m := capturedStock(t, tenant, "BOOK", 2)
	changed := m.message
	changed.TenantID = brokerTenant(t)
	if err := store.ApplyMessage(context.Background(), changed); !errors.Is(err, api.ErrSource) {
		t.Fatal("broker selected another tenant", err)
	}
	changed = m.message
	changed.Metadata.Actor = "worker:untrusted"
	if err := store.ApplyMessage(context.Background(), changed); !errors.Is(err, api.ErrSource) {
		t.Fatal("broker overrode source lineage", err)
	}
	if _, err := store.Projection(context.Background(), tenant, "BOOK"); err == nil {
		t.Fatal("unverified transport changed projection")
	}
}

func TestOrderingBrokerProjectionConvergesWithLocalRebuild(t *testing.T) {
	h := commerceHarness(t)
	tenant := brokerTenant(t)
	ctx := context.Background()
	s, _, err := (placeorder.Handler{Store: h.orders}).Handle(ctx, tenant, "place", placeorder.Command{Lines: []od.Line{{SKU: "BOOK", Quantity: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = (amendorder.Handler{Store: h.orders}).Handle(ctx, tenant, "amend", amendorder.Command{ID: s.ID, ExpectedVersion: 1, Lines: []od.Line{{SKU: "BOOK", Quantity: 2}}}); err != nil {
		t.Fatal(err)
	}
	p := &capturePublisher{}
	for range 2 {
		if did, err := h.orders.PublishStream(ctx, p, tenant, s.ID); err != nil || !did {
			t.Fatal("order publication", did, err)
		}
	}
	start := offset(t)
	stage(t, p.messages[1], start)
	stage(t, p.messages[0], start+1)
	drainInbox(t, api.HandlerFunc(func(ctx context.Context, _ string, m api.Message) error { return h.orders.ApplyMessage(ctx, m) }))
	view, err := h.orders.Projection(ctx, tenant, s.ID)
	if err != nil || view.Version != 2 || view.Lines[0].Quantity != 2 {
		t.Fatal("ordering projection", view, err)
	}
	full, err := h.orders.Rebuild(ctx, tenant, s.ID)
	if err != nil || !reflect.DeepEqual(view, full) {
		t.Fatal("rebuild diverged", err)
	}
	if err = h.orders.ApplyMessage(ctx, p.messages[0].message); err != nil {
		t.Fatal("rebuild prefix redelivery", err)
	}
}

func TestKafkaRequestPublicationCanFallBackToLocalDelivery(t *testing.T) {
	h := commerceHarness(t)
	tenant := brokerTenant(t)
	ctx := context.Background()
	product(t, h, tenant, "BOOK", "p", 100, 0)
	receive(t, tenant, "BOOK", "stock", 5)
	q, _, err := (quote.Handler{Store: h.catalog}).Handle(ctx, tenant, "q", quote.Command{Lines: []catdomain.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := (placeorder.Handler{Store: h.orders}).Handle(ctx, tenant, "order", placeorder.Command{Lines: []od.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := h.orderService.Begin(ctx, tenant, s.ID, "checkout", 1, q, "test-ok")
	if err != nil {
		t.Fatal(err)
	}
	p := &capturePublisher{}
	if did, err := h.orders.PublishCheckout(ctx, p); err != nil || !did {
		t.Fatal(did, err)
	}
	stage(t, p.messages[0], offset(t))
	fs := fulfillStore()
	handler := api.HandlerFunc(func(ctx context.Context, topic string, m api.Message) error {
		if err := h.orders.ValidateRequest(ctx, topic, m); err != nil {
			return err
		}
		if topic == api.CheckoutTopic {
			var r ordering.Requested
			if err := json.Unmarshal(m.Data, &r); err != nil {
				return err
			}
			return h.jobs.Submit(ctx, r)
		}
		var r fulfill.Request
		if err := json.Unmarshal(m.Data, &r); err != nil {
			return err
		}
		return fs.Submit(ctx, r)
	})
	drainInbox(t, handler)
	if did, err := h.orders.DispatchOne(ctx, h.jobs); err != nil || !did {
		t.Fatal("local fallback did not preserve pending request", did, err)
	}
	pumpCheckout(t, h, tenant, v.ID, "confirmed")
	p = &capturePublisher{}
	if did, err := h.orders.PublishFulfillment(ctx, p); err != nil || !did {
		t.Fatal(did, err)
	}
	stage(t, p.messages[0], offset(t))
	drainInbox(t, handler)
	if did, err := h.orders.DispatchFulfillmentOne(ctx, fs); err != nil || !did {
		t.Fatal("fulfillment local fallback", did, err)
	}
	views, err := fs.ForOrder(ctx, tenant, s.ID)
	if err != nil || len(views) != 1 {
		t.Fatal("profile fallback duplicated shipment", views, err)
	}
}

func TestBrokerPublisherCannotOvertakeLockedSourcePrefix(t *testing.T) {
	tenant := brokerTenant(t)
	ctx := context.Background()
	receive(t, tenant, "BOOK", "first", 1)
	receive(t, tenant, "BOOK", "second", 1)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT 1 FROM inventory.outbox WHERE tenant_id=$1 AND aggregate_version=1 FOR UPDATE`, tenant); err != nil {
		t.Fatal(err)
	}
	p := &capturePublisher{}
	if did, err := store.PublishOne(ctx, p); err != nil || did {
		t.Fatal("publisher overtook locked prefix", did, err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		publishInventory(t, p)
	}
	if len(p.messages) != 2 || p.messages[0].message.Sequence != 1 || p.messages[1].message.Sequence != 2 {
		t.Fatal("source ordering lost")
	}
}
