package kafka

import (
	"context"
	"errors"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func options(brokers []string) []kgo.Opt {
	return []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ClientID("atlas-go"), kgo.DialTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.RetryTimeout(3 * time.Second)}
}
func Check(ctx context.Context, brokers []string, create bool) error {
	cl, err := kgo.NewClient(options(brokers)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)
	if create {
		r, err := admin.CreateTopics(ctx, api.Partitions, 1, map[string]*string{"cleanup.policy": kadm.StringPtr("delete"), "min.insync.replicas": kadm.StringPtr("1"), "retention.ms": kadm.StringPtr("604800000")}, api.Topics...)
		if err != nil {
			return err
		}
		for _, topic := range r {
			if topic.Err != nil && !errors.Is(topic.Err, kerr.TopicAlreadyExists) {
				return topic.Err
			}
		}
	}
	topics, err := admin.ListTopics(ctx, api.Topics...)
	if err != nil {
		return err
	}
	for _, name := range api.Topics {
		d, ok := topics[name]
		if !ok || d.Err != nil || len(d.Partitions) != int(api.Partitions) {
			return errors.New("broker topic topology differs from the pinned three-partition profile")
		}
	}
	return nil
}

type Publisher struct{ Client *kgo.Client }

func NewPublisher(ctx context.Context, brokers []string) (*Publisher, error) {
	if err := Check(ctx, brokers, false); err != nil {
		return nil, err
	}
	opts := append(options(brokers), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.MaxBufferedRecords(64), kgo.ProduceRequestTimeout(time.Second), kgo.RecordDeliveryTimeout(3*time.Second))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Publisher{Client: cl}, nil
}
func (p *Publisher) Close() { p.Client.Close() }
func (p *Publisher) Publish(ctx context.Context, topic string, m api.Message) error {
	b, err := api.Encode(topic, m)
	if err != nil {
		return err
	}
	r := &kgo.Record{Topic: topic, Key: m.Key(), Value: b, Partition: api.Partition(m.Key())}
	return p.Send(ctx, r)
}

// Send is also used by failure scenarios to submit deliberately malformed wire
// records. Publication success means broker acknowledgment, not business effect.
func (p *Publisher) Send(ctx context.Context, r *kgo.Record) error {
	done := make(chan error, 1)
	p.Client.Produce(ctx, r, func(_ *kgo.Record, err error) { done <- err })
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Consumer struct {
	Brokers []string
	Group   string
	Stager  api.Stager
}

func (c Consumer) session(ctx context.Context) error {
	opts := append(options(c.Brokers), kgo.ConsumerGroup(c.Group), kgo.ConsumeTopics(api.Topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(time.Second), kgo.RebalanceTimeout(10*time.Second), kgo.FetchMaxWait(200*time.Millisecond), kgo.FetchMaxBytes(1<<20))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	for ctx.Err() == nil {
		poll, cancel := context.WithTimeout(ctx, 3*time.Second)
		f := cl.PollRecords(poll, 1)
		cancel()
		if ctx.Err() != nil {
			cl.AllowRebalance()
			return ctx.Err()
		}
		if errs := f.Errors(); len(errs) > 0 {
			cl.AllowRebalance()
			if errors.Is(errs[0].Err, context.DeadlineExceeded) {
				continue
			}
			return errs[0].Err
		}
		for _, r := range f.Records() {
			step, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = c.Stager.Stage(step, api.Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value})
			if err == nil {
				err = cl.CommitRecords(step, r)
			}
			cancel()
			cl.AllowRebalance()
			// Recreate the session from committed offsets after any failure. Polling
			// a later record here could otherwise skip a failed staging transaction.
			if err != nil {
				return err
			}
		}
		cl.AllowRebalance()
	}
	return ctx.Err()
}
func (c Consumer) Run(ctx context.Context, onError func(error)) error {
	for ctx.Err() == nil {
		err := c.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		onError(err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return ctx.Err()
}
