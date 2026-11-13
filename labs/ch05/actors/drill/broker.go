package drill

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/broker"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/checkpoint"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/contracts"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/durable"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/childprocess"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/disposabledatabase"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func keyFor(partition int32) actors.Key {
	for i := 0; i < 1000; i++ {
		key := actors.Key{Tenant: "acme", SKU: fmt.Sprintf("BOOK-%d", i)}
		if broker.Partition(key) == partition {
			return key
		}
	}
	panic("bounded partition fixture not found")
}

type consumerProcess struct {
	child *childprocess.Child
	ready string
}

func consumer(ctx context.Context, binary, dir, dsn, address, topic, replies, group, name string, reader int, before, after, marker string) (consumerProcess, error) {
	ready := filepath.Join(dir, name+"-assignment.json")
	args := []string{"consume", "-broker", address, "-topic", topic, "-replies", replies, "-group", group, "-name", name, "-reader", fmt.Sprint(reader), "-ready", ready}
	if before != "" || after != "" {
		args = append(args, "-pause-before-commit", before, "-pause-after-commit", after, "-checkpoint", marker, "-release", marker+".release")
	}
	child, e := childprocess.Start(ctx, binary, dir, map[string]string{"ATLAS_ACTORS_DATABASE_URL": dsn}, args...)
	return consumerProcess{child: child, ready: ready}, e
}
func offset(ctx context.Context, admin *kadm.Client, group, topic string, partition int32) (int64, error) {
	step, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	offsets, e := admin.FetchOffsets(step, group)
	if e != nil {
		return -1, e
	}
	value, ok := offsets.Lookup(topic, partition)
	if !ok {
		return -1, nil
	}
	if value.Err != nil {
		return -1, value.Err
	}
	return value.At, nil
}
func waitOffset(ctx context.Context, admin *kadm.Client, group string, record *kgo.Record) error {
	return wait(ctx, 20*time.Second, func() bool {
		at, e := offset(ctx, admin, group, record.Topic, record.Partition)
		return e == nil && at >= record.Offset+1
	})
}
func collect(ctx context.Context, address, topic string, count int, accept func([]byte) bool) ([][]byte, error) {
	opts := append(broker.Options(address), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()}}))
	client, e := kgo.NewClient(opts...)
	if e != nil {
		return nil, e
	}
	defer client.Close()
	step, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var out [][]byte
	seen := 0
	for len(out) < count && step.Err() == nil {
		poll, stop := context.WithTimeout(step, 500*time.Millisecond)
		f := client.PollRecords(poll, 16)
		stop()
		for _, failure := range f.Errors() {
			if !errors.Is(failure.Err, context.DeadlineExceeded) && !errors.Is(failure.Err, context.Canceled) {
				return nil, errors.New("actor observer fetch failed")
			}
		}
		for _, r := range f.Records() {
			seen++
			if seen > 128 || len(r.Value) > 8192 {
				return nil, errors.New("actor observer bound exceeded")
			}
			if accept(r.Value) {
				out = append(out, append([]byte(nil), r.Value...))
				if len(out) == count {
					return out, nil
				}
			}
		}
	}
	return out, errors.New("actor reply/relay observation did not arrive")
}
func RunBroker(ctx context.Context, dsn, address, binaries, parent string) (report Report, runErr error) {
	report.Scenario = "actors-durable-broker"
	stage := "prepare"
	defer func() {
		if runErr != nil {
			runErr = fmt.Errorf("actor broker %s: %w", stage, runErr)
		}
	}()
	binary, dir, build, e := prepare(binaries, parent)
	if e != nil {
		return report, e
	}
	report.Build = build
	if e = broker.ValidateAddress(address); e != nil {
		return report, e
	}
	db, e := disposabledatabase.Create(ctx, dsn)
	if e != nil {
		return report, e
	}
	defer func() {
		if e := db.Close(); e != nil && runErr == nil {
			runErr = e
		}
	}()
	if _, e = db.Pool.Exec(ctx, durable.Schema); e != nil {
		return report, e
	}
	store := durable.Store{Pool: db.Pool}
	producer, e := broker.Producer(address)
	if e != nil {
		return report, e
	}
	defer producer.Close()
	ping, cancel := context.WithTimeout(ctx, 10*time.Second)
	e = producer.Ping(ping)
	cancel()
	if e != nil {
		return report, errors.New("actual Kafka actor broker is unavailable; no implicit skip")
	}
	admin := kadm.NewClient(producer)
	id, e := nonce()
	if e != nil {
		return report, e
	}
	prefix := "atlas.actors." + id
	commands, replies, relay := prefix+".commands", prefix+".replies", prefix+".relay"
	group, fanout := "actors-"+id+"-main", "actors-"+id+"-fanout"
	owned := []string{}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.DeleteGroups(cleanup, group, fanout)
		if len(owned) > 0 {
			results, e := admin.DeleteTopics(cleanup, owned...)
			if e != nil && runErr == nil {
				runErr = errors.New("could not delete owned actor topics")
			}
			for _, result := range results {
				if result.Err != nil && runErr == nil {
					runErr = errors.New("owned actor topic deletion failed")
				}
			}
		}
	}()
	configs := map[string]*string{"retention.ms": kadm.StringPtr("86400000"), "cleanup.policy": kadm.StringPtr("delete"), "max.message.bytes": kadm.StringPtr("8192"), "segment.bytes": kadm.StringPtr("1048576")}
	for _, topic := range []string{commands, replies, relay} {
		created, e := admin.CreateTopics(ctx, broker.Partitions, 1, configs, topic)
		if e != nil {
			return report, e
		}
		result, ok := created[topic]
		if !ok || result.Err != nil {
			return report, errors.New("could not create fresh owned actor topic")
		}
		owned = append(owned, topic)
	}
	stage = "producer buffering without recipient"
	keys := make([]actors.Key, 3)
	seeds := make([][]byte, 3)
	records := make([]*kgo.Record, 3)
	for partition := int32(0); partition < 3; partition++ {
		keys[partition] = keyFor(partition)
		seeds[partition], e = raw(2, fmt.Sprintf("stock-%d", partition), keys[partition], "receive.v1", 10+int64(partition))
		if e != nil {
			return report, e
		}
		records[partition], e = broker.Send(ctx, producer, commands, keys[partition], seeds[partition])
		if e != nil {
			return report, e
		}
	}
	var rows int
	if e = db.Pool.QueryRow(ctx, `SELECT count(*) FROM actors.stock`).Scan(&rows); e != nil {
		return report, e
	}
	if e = report.check("broker acknowledges bytes while consumers are absent", "three real records; no business rows yet", map[string]any{"records": len(records), "stock_rows": rows}, len(records) == 3 && rows == 0); e != nil {
		return report, e
	}
	stage = "consumer SIGKILL before SQL commit"
	beforeMarker := filepath.Join(dir, "before-commit.json")
	before, e := consumer(ctx, binary, dir, db.DSN, address, commands, replies, group, "precommit", 1, "stock-0", "", beforeMarker)
	if e != nil {
		return report, e
	}
	defer before.child.Stop()
	if e = waitCheckpoint(ctx, beforeMarker, "before-commit", "stock-0"); e != nil {
		return report, e
	}
	var count int
	if e = db.Pool.QueryRow(ctx, `SELECT count(*) FROM actors.receipts WHERE subscription=$1 AND message_id='stock-0'`, group).Scan(&count); e != nil {
		return report, e
	}
	at, e := offset(ctx, admin, group, commands, 0)
	if e != nil {
		return report, e
	}
	if e = report.check("before-commit consumer has no durable outcome or offset", "0 saved receipts; source remains unacknowledged", map[string]any{"receipts": count, "offset": at}, count == 0 && at <= records[0].Offset); e != nil {
		return report, e
	}
	before.child.Kill()
	if e = wait(ctx, 5*time.Second, func() bool {
		var n int
		e := db.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='atlas-actors/precommit'`).Scan(&n)
		return e == nil && n == 0
	}); e != nil {
		return report, e
	}
	state, e := store.State(ctx, group, keys[0])
	if e != nil {
		return report, e
	}
	if e = report.check("real SIGKILL before commit rolls back actor effect", "target state remains zero after database session drains", state, state.OnHand == 0 && state.Version == 0); e != nil {
		return report, e
	}
	stage = "consumer SIGKILL after SQL commit before Kafka offset"
	afterMarker := filepath.Join(dir, "after-commit.json")
	after, e := consumer(ctx, binary, dir, db.DSN, address, commands, replies, group, "postcommit", 1, "", "stock-0", afterMarker)
	if e != nil {
		return report, e
	}
	defer after.child.Stop()
	if e = waitCheckpoint(ctx, afterMarker, "after-commit-before-offset", "stock-0"); e != nil {
		return report, e
	}
	state, e = store.State(ctx, group, keys[0])
	if e != nil {
		return report, e
	}
	if e = db.Pool.QueryRow(ctx, `SELECT count(*) FROM actors.receipts WHERE subscription=$1 AND message_id='stock-0'`, group).Scan(&count); e != nil {
		return report, e
	}
	at, e = offset(ctx, admin, group, commands, 0)
	if e != nil {
		return report, e
	}
	if e = report.check("durable effect and receipt precede source acknowledgment", "stock 10/version 1 and one receipt; no source offset commit", map[string]any{"state": state, "receipts": count, "offset": at}, state.OnHand == 10 && state.Version == 1 && count == 1 && at <= records[0].Offset); e != nil {
		return report, e
	}
	after.child.Kill()
	stage = "replacement consumer resolves redelivery"
	recovered, e := consumer(ctx, binary, dir, db.DSN, address, commands, replies, group, "recovery", 2, "", "", "")
	if e != nil {
		return report, e
	}
	defer recovered.child.Stop()
	for _, record := range records {
		if e = waitOffset(ctx, admin, group, record); e != nil {
			return report, e
		}
	}
	var attempts, replayed int
	if e = db.Pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE replayed) FROM actors.attempts WHERE subscription=$1 AND message_id='stock-0'`, group).Scan(&attempts, &replayed); e != nil {
		return report, e
	}
	state, e = store.State(ctx, group, keys[0])
	if e != nil {
		return report, e
	}
	if e = report.check("Kafka redelivers after committed consumer dies", "at least two processing attempts, one stock effect", map[string]any{"attempts": attempts, "replayed_attempts": replayed, "state": state}, attempts >= 2 && replayed >= 1 && state.OnHand == 10 && state.Version == 1); e != nil {
		return report, e
	}
	var source []byte
	if e = db.Pool.QueryRow(ctx, `SELECT source_body FROM actors.receipts WHERE subscription=$1 AND message_id='stock-0'`, group).Scan(&source); e != nil {
		return report, e
	}
	if e = report.check("old consumer preserves original unknown wire bytes", "source body equals the current writer's complete original record", fmt.Sprintf("%x", source), bytes.Equal(source, seeds[0])); e != nil {
		return report, e
	}
	stage = "competing consumers share one subscription"
	peer, e := consumer(ctx, binary, dir, db.DSN, address, commands, replies, group, "peer", 1, "", "", "")
	if e != nil {
		return report, e
	}
	defer peer.child.Stop()
	var assignmentA, assignmentB checkpoint.Event
	if e = wait(ctx, 20*time.Second, func() bool {
		var ea, eb error
		assignmentA, ea = checkpoint.Read(recovered.ready)
		assignmentB, eb = checkpoint.Read(peer.ready)
		if ea != nil || eb != nil || assignmentA.Stage != "assigned" || assignmentB.Stage != "assigned" || len(assignmentA.Partitions) == 0 || len(assignmentB.Partitions) == 0 {
			return false
		}
		seen := map[int32]bool{}
		for _, p := range assignmentA.Partitions {
			seen[p] = true
		}
		for _, p := range assignmentB.Partitions {
			if seen[p] {
				return false
			}
			seen[p] = true
		}
		return len(seen) == 3
	}); e != nil {
		return report, e
	}
	if e = report.check("consumer group distributes partitions among workers", "three partitions with disjoint nonempty assignments", []checkpoint.Event{assignmentA, assignmentB}, true); e != nil {
		return report, e
	}
	latest := make([]*kgo.Record, 3)
	for p := 0; p < 3; p++ {
		take, e := raw(2, fmt.Sprintf("take-%d", p), keys[p], "reserve.v1", 2)
		if e != nil {
			return report, e
		}
		latest[p], e = broker.Send(ctx, producer, commands, keys[p], take)
		if e != nil {
			return report, e
		}
	}
	for _, record := range latest {
		if e = waitOffset(ctx, admin, group, record); e != nil {
			return report, e
		}
	}
	var workers int
	if e = db.Pool.QueryRow(ctx, `SELECT count(DISTINCT worker) FROM actors.attempts WHERE subscription=$1 AND message_id LIKE 'take-%' AND NOT replayed`, group).Scan(&workers); e != nil {
		return report, e
	}
	if e = report.check("competing consumers each process distinct commands", "both actual processes contribute local effects", workers, workers == 2); e != nil {
		return report, e
	}
	stage = "request/reply over a separate Kafka channel"
	replyBytes, e := collect(ctx, address, replies, 1, func(raw []byte) bool {
		var response broker.Response
		return json.Unmarshal(raw, &response) == nil && response.Subscription == group && response.ID == "take-0"
	})
	if e != nil {
		return report, e
	}
	var response broker.Response
	if e = json.Unmarshal(replyBytes[0], &response); e != nil {
		return report, e
	}
	if e = report.check("synchronous-style result correlates on a separate reply topic", "reply ID/key/subscription match the command; stock reserved once", response, response.Format == 1 && response.ID == "take-0" && response.Key == keys[0] && response.Reply.State.Reserved == 2); e != nil {
		return report, e
	}
	stage = "physical duplicates preserve logical identity"
	duplicate, e := broker.Send(ctx, producer, commands, keys[0], seeds[0])
	if e != nil {
		return report, e
	}
	latest[0] = duplicate
	if e = waitOffset(ctx, admin, group, duplicate); e != nil {
		return report, e
	}
	state, e = store.State(ctx, group, keys[0])
	if e != nil {
		return report, e
	}
	if e = report.check("two Kafka records can represent one actor command", "new physical offset; no second receive effect", map[string]any{"first_offset": records[0].Offset, "duplicate_offset": duplicate.Offset, "state": state}, duplicate.Offset > records[0].Offset && state.OnHand == 10 && state.Reserved == 2 && state.Version == 2); e != nil {
		return report, e
	}
	stage = "independent subscriber fan-out and retained history"
	subscriber, e := consumer(ctx, binary, dir, db.DSN, address, commands, replies, fanout, "fanout", 2, "", "", "")
	if e != nil {
		return report, e
	}
	defer subscriber.child.Stop()
	for _, record := range latest {
		if e = waitOffset(ctx, admin, fanout, record); e != nil {
			return report, e
		}
	}
	states := []actors.State{}
	for p, key := range keys {
		a, e := store.State(ctx, group, key)
		if e != nil {
			return report, e
		}
		b, e := store.State(ctx, fanout, key)
		if e != nil {
			return report, e
		}
		if a != b || a.OnHand != int64(10+p) || a.Reserved != 2 || a.Version != 2 {
			return report, errors.New("independent subscriber state diverged")
		}
		states = append(states, b)
	}
	if e = report.check("separate consumer groups each receive retained topic history", "all three entity states agree in independent subscriptions", states, len(states) == 3); e != nil {
		return report, e
	}
	var mainReceipts, fanoutReceipts int
	if e = db.Pool.QueryRow(ctx, `SELECT count(*) FROM actors.receipts WHERE subscription=$1`, group).Scan(&mainReceipts); e != nil {
		return report, e
	}
	if e = db.Pool.QueryRow(ctx, `SELECT count(*) FROM actors.receipts WHERE subscription=$1`, fanout).Scan(&fanoutReceipts); e != nil {
		return report, e
	}
	if e = report.check("transport redelivery is distinct from durable local effects", "six logical receipts per subscription despite retries and duplicate physical records", []int{mainReceipts, fanoutReceipts}, mainReceipts == 6 && fanoutReceipts == 6); e != nil {
		return report, e
	}
	stage = "actual broker relay preserves unknown payload fields"
	preserved, e := actors.PreserveRelay(seeds[0])
	if e != nil {
		return report, e
	}
	lossy, e := actors.TypedOldRelay(seeds[0])
	if e != nil {
		return report, e
	}
	if _, e = broker.Send(ctx, producer, relay, keys[0], preserved); e != nil {
		return report, e
	}
	if _, e = broker.Send(ctx, producer, relay, keys[0], lossy); e != nil {
		return report, e
	}
	relayed, e := collect(ctx, address, relay, 2, func([]byte) bool { return true })
	if e != nil {
		return report, e
	}
	kept, e := actors.Decode(relayed[0], 2)
	if e != nil {
		return report, e
	}
	lost, e := actors.Decode(relayed[1], 2)
	if e != nil {
		return report, e
	}
	if e = report.check("old consumer relay must retain original encoded payload", "raw relay retains note; typed old reconstruction loses it", map[string]any{"preserved_note": kept.Note, "lost_note": lost.Note}, bytes.Equal(relayed[0], seeds[0]) && kept.Note == "v2 annotation" && lost.Note == ""); e != nil {
		return report, e
	}
	_, conflict := store.Apply(ctx, group, "relay-check", lost, durable.Source{Topic: relay, Partition: 0, Offset: 1})
	if e = report.check("receipt identity detects a rewritten unknown field", "changed canonical body conflicts instead of replacing saved history", fmt.Sprint(conflict), errors.Is(conflict, actors.ErrIdentityConflict)); e != nil {
		return report, e
	}
	if e = contracts.Validate("Command", seeds[0]); e != nil {
		return report, e
	}
	if e = contracts.Validate("Response", replyBytes[0]); e != nil {
		return report, e
	}
	if e = report.check("actual commands and replies satisfy AsyncAPI payload contracts", "authored schemas validate native published values", 2, true); e != nil {
		return report, e
	}
	report.Verified = true
	report.Boundaries = []string{"One real Kafka broker with replication factor 1 and all-ISR acknowledgments; no replicated durability or power-loss proof.", "Topic retention is 24 hours and delete policy, not an indefinite event store.", "Kafka source offsets, reply publication and PostgreSQL transactions are separate boundaries. Duplicate replies still need correlation/retention policy.", "Actor serialization is local; PostgreSQL row locks and complete message receipts protect shared durable effects.", "The fixture owns its database, three fresh topics, scoped groups and actual child processes; failures never silently skip native services."}
	return report, nil
}
