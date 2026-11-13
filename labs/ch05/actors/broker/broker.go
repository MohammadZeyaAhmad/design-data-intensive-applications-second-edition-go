// Package broker connects actors to actual Kafka delivery. Broker offsets and
// PostgreSQL receipts are deliberately separate acknowledgment boundaries.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/checkpoint"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/contracts"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/durable"
	"github.com/twmb/franz-go/pkg/kgo"
)

const Partitions int32 = 3

func ValidateAddress(address string) error {
	host, port, e := net.SplitHostPort(address)
	if e != nil || host != "127.0.0.1" || (port != "19092" && port != "19093" && port != "19094") {
		return errors.New("actor broker must be a local test/development Kafka listener at 19092..19094")
	}
	return nil
}
func Partition(key actors.Key) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key.Normalize().String()))
	return int32(h.Sum32() % uint32(Partitions))
}
func Options(address string) []kgo.Opt {
	return []kgo.Opt{kgo.SeedBrokers(address), kgo.DialTimeout(2 * time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.FetchMaxWait(100 * time.Millisecond), kgo.FetchMaxBytes(64 << 10), kgo.FetchMaxPartitionBytes(16 << 10)}
}
func Producer(address string) (*kgo.Client, error) {
	if e := ValidateAddress(address); e != nil {
		return nil, e
	}
	opts := append(Options(address), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.MaxBufferedRecords(32), kgo.MaxBufferedBytes(256<<10), kgo.ProduceRequestTimeout(time.Second), kgo.RecordDeliveryTimeout(4*time.Second))
	return kgo.NewClient(opts...)
}
func Send(parent context.Context, producer *kgo.Client, topic string, key actors.Key, raw []byte) (*kgo.Record, error) {
	if len(raw) > 8192 {
		return nil, actors.ErrInvalid
	}
	schema := "Command"
	if strings.HasSuffix(topic, ".replies") {
		schema = "Response"
	}
	if schema == "Command" {
		m, e := actors.Decode(raw, 2)
		if e != nil {
			return nil, e
		}
		if m.Key != key.Normalize() {
			return nil, actors.ErrInvalid
		}
	} else {
		var reply Response
		if json.Unmarshal(raw, &reply) != nil || reply.Key != key.Normalize() {
			return nil, actors.ErrInvalid
		}
	}
	if e := contracts.Validate(schema, raw); e != nil {
		return nil, e
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	record := &kgo.Record{Topic: topic, Partition: Partition(key), Key: []byte(key.Normalize().String()), Value: append([]byte(nil), raw...)}
	if e := producer.ProduceSync(ctx, record).FirstErr(); e != nil {
		return nil, e
	}
	return record, nil
}

type Response struct {
	Format       int          `json:"format"`
	ID           string       `json:"id"`
	Key          actors.Key   `json:"key"`
	Subscription string       `json:"subscription"`
	Reply        actors.Reply `json:"reply"`
}
type Config struct {
	Address, Topic, Replies, Group, Name, Ready, BeforeCommitID, AfterCommitID, Checkpoint, Release string
	Reader                                                                                          int
	Store                                                                                           durable.Store
}

var topicPattern = regexp.MustCompile(`^atlas\.actors\.[0-9a-f]{24}\.(commands|replies|relay)$`)
var groupPattern = regexp.MustCompile(`^actors-[0-9a-f]{24}-(main|fanout)$`)

func Run(ctx context.Context, c Config) error {
	if e := ValidateAddress(c.Address); e != nil {
		return e
	}
	if !topicPattern.MatchString(c.Topic) || !topicPattern.MatchString(c.Replies) || !strings.HasSuffix(c.Topic, ".commands") || !strings.HasSuffix(c.Replies, ".replies") || strings.TrimSuffix(c.Topic, ".commands") != strings.TrimSuffix(c.Replies, ".replies") || !groupPattern.MatchString(c.Group) || c.Name == "" || c.Store.Pool == nil {
		return actors.ErrInvalid
	}
	producer, e := Producer(c.Address)
	if e != nil {
		return e
	}
	defer producer.Close()
	if c.BeforeCommitID != "" {
		c.Store.BeforeCommit = func(ctx context.Context, m actors.Message) error {
			if m.ID == c.BeforeCommitID {
				return checkpoint.Pause(ctx, c.Checkpoint, c.Release, checkpoint.Event{Stage: "before-commit", ID: m.ID, Worker: c.Name})
			}
			return nil
		}
	}
	o := actors.DefaultOptions()
	o.Reader = c.Reader
	o.Factory = func(actors.Key) actors.Processor {
		return func(ctx context.Context, m actors.Message) (actors.Reply, error) {
			position := actors.PositionFrom(ctx)
			return c.Store.Apply(ctx, c.Group, c.Name, m, durable.Source{Topic: position.Topic, Partition: position.Partition, Offset: position.Offset})
		}
	}
	runtime, e := actors.New(ctx, o)
	if e != nil {
		return e
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Close(stop)
	}()
	callbackError := make(chan error, 1)
	var assignmentMu sync.Mutex
	owned := map[int32]bool{}
	writeAssignment := func(stage string, partitions map[string][]int32) {
		assignmentMu.Lock()
		defer assignmentMu.Unlock()
		for _, p := range partitions[c.Topic] {
			if stage == "assigned" {
				owned[p] = true
			} else {
				delete(owned, p)
			}
		}
		snapshot := make([]int32, 0, len(owned))
		for p := range owned {
			snapshot = append(snapshot, p)
		}
		sort.Slice(snapshot, func(i, j int) bool { return snapshot[i] < snapshot[j] })
		state := "assigned"
		if len(snapshot) == 0 {
			state = "unassigned"
		}
		if e := checkpoint.Write(c.Ready, checkpoint.Event{Stage: state, Worker: c.Name, Partitions: snapshot}); e != nil {
			select {
			case callbackError <- e:
			default:
			}
		}
	}
	opts := append(Options(c.Address), kgo.ConsumerGroup(c.Group), kgo.ConsumeTopics(c.Topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(time.Second), kgo.RebalanceTimeout(10*time.Second), kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { writeAssignment("assigned", p) }), kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { writeAssignment("revoked", p) }), kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { writeAssignment("lost", p) }))
	consumer, e := kgo.NewClient(opts...)
	if e != nil {
		return e
	}
	defer consumer.CloseAllowingRebalance()
	for ctx.Err() == nil {
		select {
		case e := <-callbackError:
			return e
		default:
		}
		poll, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := consumer.PollRecords(poll, 1)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		for _, failure := range fetches.Errors() {
			if !errors.Is(failure.Err, context.DeadlineExceeded) && !errors.Is(failure.Err, context.Canceled) {
				consumer.AllowRebalance()
				return errors.New("actor consumer fetch failed")
			}
		}
		for _, record := range fetches.Records() {
			m, e := actors.Decode(record.Value, c.Reader)
			if e != nil {
				consumer.AllowRebalance()
				return e
			}
			if m.Kind == "get.v1" || record.Partition != Partition(m.Key) || string(record.Key) != m.Key.String() {
				consumer.AllowRebalance()
				return actors.ErrInvalid
			}
			ref, e := runtime.Resolve(m.Key)
			if e != nil {
				consumer.AllowRebalance()
				return e
			}
			processing := actors.WithPosition(ctx, actors.Position{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset})
			out, e := runtime.Ask(processing, ref, record.Value)
			if e != nil {
				consumer.AllowRebalance()
				return e
			}
			if m.ID == c.AfterCommitID {
				if e = checkpoint.Pause(ctx, c.Checkpoint, c.Release, checkpoint.Event{Stage: "after-commit-before-offset", ID: m.ID, Worker: c.Name}); e != nil {
					consumer.AllowRebalance()
					return e
				}
			}
			response, e := json.Marshal(Response{Format: 1, ID: m.ID, Key: m.Key, Subscription: c.Group, Reply: out})
			if e != nil {
				consumer.AllowRebalance()
				return e
			}
			if _, e = Send(ctx, producer, c.Replies, m.Key, response); e != nil {
				consumer.AllowRebalance()
				return e
			}
			// Never acknowledge the source before the local effect+receipt commit.
			// A crash in this remaining window replays the saved business outcome.
			if e = consumer.CommitRecords(ctx, record); e != nil {
				consumer.AllowRebalance()
				return e
			}
		}
		consumer.AllowRebalance()
	}
	return nil
}
