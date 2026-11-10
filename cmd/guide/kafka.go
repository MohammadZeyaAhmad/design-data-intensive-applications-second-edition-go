package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/messaging/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func waitKafkaCheck(check func() (bool, error)) error {
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		yes, err := check()
		if err != nil {
			return err
		}
		if yes {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("Kafka recovery condition did not converge")
}
func stockAtVersion(c *client, sku string, version int64) error {
	return waitKafkaCheck(func() (bool, error) {
		code, b, err := c.request("GET", "/v1/stock/"+sku+"/projection", "acme", "", nil)
		if err != nil {
			return false, err
		}
		if code == 404 {
			return false, nil
		}
		if code != 200 {
			return false, errors.New("projection read failed")
		}
		var state domain.StockState
		if err = json.Unmarshal(b, &state); err != nil {
			return false, err
		}
		return state.Version == version, nil
	})
}
func verifyKafka(c *client) (result error) {
	u, _ := url.Parse(c.base)
	if os.Getenv("ATLAS_MESSAGE_TRANSPORT") != "kafka" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
		return errors.New("run make up-kafka and make verify-kafka against the local development stack")
	}
	pool, err := pgxpool.New(context.Background(), os.Getenv("ATLAS_DATABASE_URL"))
	if err != nil {
		return errors.New("local verification database unavailable")
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	name, err := sku("kafka-")
	if err != nil {
		return err
	}
	if err = compose("stop", "broker-consumer"); err != nil {
		return err
	}
	restored := false
	defer func() {
		if !restored {
			result = errors.Join(result, compose("start", "kafka", "broker-publisher", "broker-consumer"))
		}
	}()
	if _, err = c.expect("POST", "/v1/stock/"+name+"/receipts", "acme", name+"-first", map[string]int{"quantity": 5}, 200); err != nil {
		return err
	}
	if err = waitKafkaCheck(func() (bool, error) {
		var yes bool
		err := pool.QueryRow(ctx, `SELECT broker_published_at IS NOT NULL FROM inventory.outbox WHERE tenant_id='acme' AND sku=$1 AND aggregate_version=1`, name).Scan(&yes)
		return yes, err
	}); err != nil {
		return err
	}
	if _, err = c.expect("GET", "/v1/stock/"+name+"/projection", "acme", "", nil, 404); err != nil {
		return err
	}
	if err = compose("start", "broker-consumer"); err != nil {
		return err
	}
	if err = stockAtVersion(c, name, 1); err != nil {
		return err
	}
	if err = compose("stop", "kafka"); err != nil {
		return err
	}
	if _, err = c.expect("POST", "/v1/stock/"+name+"/receipts", "acme", name+"-second", map[string]int{"quantity": 3}, 200); err != nil {
		return err
	}
	var unpublished bool
	if err = pool.QueryRow(ctx, `SELECT broker_published_at IS NULL FROM inventory.outbox WHERE tenant_id='acme' AND sku=$1 AND aggregate_version=2`, name).Scan(&unpublished); err != nil || !unpublished {
		return errors.New("broker outage lost the pending source record")
	}
	if err = compose("kill", "-s", "KILL", "broker-publisher"); err != nil {
		return err
	}
	if err = compose("start", "kafka", "broker-publisher", "broker-consumer"); err != nil {
		return err
	}
	restored = true
	if err = stockAtVersion(c, name, 2); err != nil {
		return err
	}
	p, err := kafka.NewPublisher(ctx, []string{"127.0.0.1:19092"})
	if err != nil {
		return err
	}
	defer p.Close()
	m := api.Message{Version: 1, SchemaVersion: 1, StreamID: api.InventoryStream(name, "")}
	if err = pool.QueryRow(ctx, `SELECT event_id::text,tenant_id,aggregate_version,event_type,payload,metadata FROM inventory.outbox WHERE tenant_id='acme' AND sku=$1 AND aggregate_version=1`, name).Scan(&m.ID, &m.TenantID, &m.Sequence, &m.Type, &m.Data, &m.Metadata); err != nil {
		return err
	}
	send := func(value []byte) (*kgo.Record, error) {
		r := &kgo.Record{Topic: api.InventoryTopic, Key: m.Key(), Partition: api.Partition(m.Key()), Value: value}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return r, p.Send(step, r)
	}
	encoded, _ := api.Encode(api.InventoryTopic, m)
	duplicate, err := send(encoded)
	if err != nil {
		return err
	}
	if err = waitKafkaCheck(func() (bool, error) {
		var seen bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM messaging.deliveries WHERE group_id=$1 AND topic=$2 AND partition=$3 AND offset_id=$4)`, api.Group, api.InventoryTopic, duplicate.Partition, duplicate.Offset).Scan(&seen)
		return seen, err
	}); err != nil {
		return err
	}
	bad, err := send([]byte(`{"version":99}`))
	if err != nil {
		return err
	}
	if err = waitKafkaCheck(func() (bool, error) {
		var seen bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM messaging.rejected WHERE group_id=$1 AND topic=$2 AND partition=$3 AND offset_id=$4 AND reason='invalid_envelope')`, api.Group, api.InventoryTopic, bad.Partition, bad.Offset).Scan(&seen)
		return seen, err
	}); err != nil {
		return err
	}
	conflict := m
	conflict.Type = "inventory.conflicting-fixture"
	b, _ := api.Encode(api.InventoryTopic, conflict)
	if _, err = send(b); err != nil {
		return err
	}
	if err = waitKafkaCheck(func() (bool, error) {
		var status string
		err := pool.QueryRow(ctx, `SELECT status FROM messaging.inbox WHERE topic=$1 AND message_id=$2::uuid`, api.InventoryTopic, m.ID).Scan(&status)
		return status == "quarantined", err
	}); err != nil {
		return err
	}
	if _, err = c.expect("POST", "/v1/stock/"+name+"/receipts", "acme", name+"-third", map[string]int{"quantity": 2}, 200); err != nil {
		return err
	}
	if err = waitKafkaCheck(func() (bool, error) {
		var seen bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM messaging.inbox WHERE topic=$1 AND tenant_id='acme' AND stream_id=$2 AND sequence=3 AND status='pending')`, api.InventoryTopic, m.StreamID).Scan(&seen)
		return seen, err
	}); err != nil {
		return err
	}
	b, err = c.expect("GET", "/v1/stock/"+name+"/projection", "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var held domain.StockState
	if err = json.Unmarshal(b, &held); err != nil || held.Version != 2 {
		return errors.New("conflicting stream was not fenced")
	}
	binary, err := filepath.Abs("artifacts/admin-verification")
	if err != nil {
		return err
	}
	if err = exec.Command("go", "build", "-o", binary, "./cmd/admin").Run(); err != nil {
		return errors.New("operator build failed")
	}
	correlation, err := identity.NewID()
	if err != nil {
		return err
	}
	admin := adminClient{binary: binary, correlation: correlation}
	args := []string{"-topic", api.InventoryTopic, "-reason", "source_verified", "-key", m.ID + "-verified", "-expected-control-version", "0"}
	first, err := admin.call("inbox-retry", "", m.ID, args...)
	if err != nil {
		return err
	}
	again, err := admin.call("inbox-retry", "", m.ID, args...)
	if err != nil {
		return err
	}
	var a, both struct {
		Replayed bool            `json:"replayed"`
		Result   json.RawMessage `json:"result"`
	}
	if json.Unmarshal(first, &a) != nil || json.Unmarshal(again, &both) != nil || a.Replayed || !both.Replayed || !api.Same(a.Result, both.Result) {
		return errors.New("inbox retry did not preserve its audit receipt")
	}
	if err = stockAtVersion(c, name, 3); err != nil {
		return err
	}
	var receipts int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM inventory.projection_receipts r JOIN inventory.outbox o ON o.event_id=r.event_id WHERE o.tenant_id='acme' AND o.sku=$1`, name).Scan(&receipts); err != nil || receipts != 3 {
		return errors.New("Kafka replay duplicated a local business effect")
	}
	return report("kafka", map[string]any{"passed": true, "broker_ack_is_separate_from_application": true, "broker_outage_preserves_source_commands": true, "publisher_kill_recovers_pending_work": true, "duplicate_wire_has_one_owner_effect": true, "invalid_wire_is_durably_rejected": true, "identity_conflict_fences_only_its_stream": true, "scoped_retry_preserves_message_identity": true})
}
