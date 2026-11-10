//go:build integration && kafka

package integration

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	broker "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/messaging/kafka"
	inboxdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/messaging/postgres"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func kafkaFixture(t *testing.T) ([]string, *broker.Publisher, *kadm.Client) {
	t.Helper()
	raw := os.Getenv("ATLAS_KAFKA_TEST_BROKERS")
	if raw != "127.0.0.1:19093" {
		t.Fatal("real Kafka tests require the dedicated local test broker at 127.0.0.1:19093")
	}
	addresses := []string{raw}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := broker.Check(ctx, addresses, true); err != nil {
		t.Fatal(err)
	}
	p, err := broker.NewPublisher(ctx, addresses)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return addresses, p, kadm.NewClient(p.Client)
}

type realAckLoss struct {
	target  *broker.Publisher
	records []*kgo.Record
	lose    bool
}

func (p *realAckLoss) Publish(ctx context.Context, topic string, m api.Message) error {
	b, err := api.Encode(topic, m)
	if err != nil {
		return err
	}
	r := &kgo.Record{Topic: topic, Key: m.Key(), Partition: api.Partition(m.Key()), Value: b}
	if err = p.target.Send(ctx, r); err != nil {
		return err
	}
	p.records = append(p.records, r)
	if p.lose {
		p.lose = false
		return errors.New("injected loss after real Kafka acknowledgment")
	}
	return nil
}

type persistedBeforeAck struct {
	target inboxdb.Inbox
	tenant string
	lose   atomic.Bool
	seen   atomic.Int64
}

func (p *persistedBeforeAck) Stage(ctx context.Context, r api.Record) error {
	if err := p.target.Stage(ctx, r); err != nil {
		return err
	}
	m, err := api.Decode(r.Topic, r.Value)
	if err == nil && m.TenantID == p.tenant {
		p.seen.Add(1)
		if p.lose.Swap(false) {
			return errors.New("injected crash after durable staging before Kafka commit")
		}
	}
	return nil
}
func runConsumer(t *testing.T, c broker.Consumer) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, func(error) {}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Kafka consumer did not stop")
		}
	})
	return cancel, done
}
func waitCondition(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("Kafka condition did not converge")
}
func fixtureHandler(tenant string) api.Handler {
	// The isolated broker retains synthetic records from earlier test databases.
	// Persist every delivery; this fixture's application subscribes to one tenant.
	return api.HandlerFunc(func(ctx context.Context, topic string, m api.Message) error {
		if m.TenantID != tenant {
			return nil
		}
		return store.ApplyMessage(ctx, m)
	})
}
func TestKafkaRealAckLossAndConsumerCheckpointLoss(t *testing.T) {
	addresses, p, admin := kafkaFixture(t)
	tenant := brokerTenant(t)
	ctx := context.Background()
	receive(t, tenant, "BOOK", "stock", 7)
	recorder := &realAckLoss{target: p, lose: true}
	if did, err := store.PublishOne(ctx, recorder); !did || err == nil {
		t.Fatal("real publication boundary not injected")
	}
	if !publishInventory(t, recorder) || len(recorder.records) != 2 {
		t.Fatal("source did not retry")
	}
	first, last := recorder.records[0], recorder.records[1]
	if first.Offset >= last.Offset || first.Partition != last.Partition || !api.Same(first.Value, last.Value) {
		t.Fatal("expected two physical records for one logical event")
	}
	group := "atlas-test-" + newID(t)
	stager := &persistedBeforeAck{target: inboxdb.Inbox{Pool: pool, Group: group}, tenant: tenant}
	stager.lose.Store(true)
	runConsumer(t, broker.Consumer{Brokers: addresses, Group: group, Stager: stager})
	waitCondition(t, func() bool {
		step, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		offsets, err := admin.FetchOffsets(step, group)
		if err != nil {
			return false
		}
		offset, ok := offsets.Lookup(api.InventoryTopic, last.Partition)
		return ok && offset.Err == nil && offset.At >= last.Offset+1
	})
	if stager.seen.Load() < 3 {
		t.Fatal("durable receipt was not replayed after lost offset commit", stager.seen.Load())
	}
	drainInbox(t, fixtureHandler(tenant))
	view, err := store.Projection(ctx, tenant, "BOOK")
	if err != nil || view.OnHand != 7 || view.Version != 1 {
		t.Fatal("real broker duplicated effect", view, err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM inventory.projection_receipts WHERE tenant_id=$1`, tenant).Scan(&n); err != nil || n != 1 {
		t.Fatal("owner receipt count", n, err)
	}
}
func TestKafkaConsumerGroupRebalanceRetainsStagedPrefix(t *testing.T) {
	addresses, p, admin := kafkaFixture(t)
	tenant := brokerTenant(t)
	ctx := context.Background()
	recorder := &realAckLoss{target: p}
	for i := range 12 {
		receive(t, tenant, "BOOK", string(rune('a'+i)), 1)
		publishInventory(t, recorder)
	}
	group := "atlas-rebalance-" + newID(t)
	s := &persistedBeforeAck{target: inboxdb.Inbox{Pool: pool, Group: group}, tenant: tenant}
	cancel1, _ := runConsumer(t, broker.Consumer{Brokers: addresses, Group: group, Stager: s})
	waitCondition(t, func() bool { return s.seen.Load() >= 12 })
	runConsumer(t, broker.Consumer{Brokers: addresses, Group: group, Stager: s})
	waitCondition(t, func() bool {
		step, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		groups, err := admin.DescribeGroups(step, group)
		g := groups[group]
		return err == nil && g.Err == nil && g.State == "Stable" && len(g.Members) == 2
	})
	cancel1()
	for i := range 6 {
		receive(t, tenant, "BOOK", string(rune('m'+i)), 1)
		publishInventory(t, recorder)
	}
	last := recorder.records[len(recorder.records)-1]
	waitCondition(t, func() bool {
		step, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		offsets, err := admin.FetchOffsets(step, group)
		o, ok := offsets.Lookup(api.InventoryTopic, last.Partition)
		return err == nil && ok && o.At >= last.Offset+1
	})
	drainInbox(t, fixtureHandler(tenant))
	view, err := store.Projection(ctx, tenant, "BOOK")
	if err != nil || view.Version != 18 || view.OnHand != 18 {
		t.Fatal("rebalance lost or reordered a committed source effect", view, err)
	}
}
func TestKafkaConsumerReplaysAfterDatabaseStageFailure(t *testing.T) {
	addresses, p, admin := kafkaFixture(t)
	tenant := brokerTenant(t)
	ctx := context.Background()
	receive(t, tenant, "BOOK", "stock", 3)
	recorder := &realAckLoss{target: p}
	publishInventory(t, recorder)
	group := "atlas-stage-fail-" + newID(t)
	target := inboxdb.Inbox{Pool: pool, Group: group}
	failed := atomic.Bool{}
	stager := stageFunc(func(ctx context.Context, r api.Record) error {
		m, _ := api.Decode(r.Topic, r.Value)
		if m.TenantID == tenant && !failed.Swap(true) {
			return errors.New("injected database unavailable before receipt")
		}
		return target.Stage(ctx, r)
	})
	runConsumer(t, broker.Consumer{Brokers: addresses, Group: group, Stager: stager})
	last := recorder.records[0]
	waitCondition(t, func() bool {
		step, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		offsets, err := admin.FetchOffsets(step, group)
		o, ok := offsets.Lookup(api.InventoryTopic, last.Partition)
		return err == nil && ok && o.At >= last.Offset+1
	})
	drainInbox(t, fixtureHandler(tenant))
	view, err := store.Projection(ctx, tenant, "BOOK")
	if err != nil || view.OnHand != 3 || !failed.Load() {
		t.Fatal("failed staging advanced the broker offset", view, err)
	}
}

type stageFunc func(context.Context, api.Record) error

func (f stageFunc) Stage(ctx context.Context, r api.Record) error { return f(ctx, r) }
