package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"regexp"
	"strings"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
)

const (
	InventoryTopic         = "atlas.inventory.v1"
	OrderingTopic          = "atlas.orders.v1"
	CheckoutTopic          = "atlas.checkout.v1"
	FulfillmentTopic       = "atlas.fulfillment.v1"
	Partitions       int32 = 3
	MaxBytes               = 256 << 10
	Group                  = "atlas-durable-inbox-v1"
)

var Topics = []string{InventoryTopic, OrderingTopic, CheckoutTopic, FulfillmentTopic}
var ErrInvalid = errors.New("invalid transport envelope")
var ErrSource = errors.New("transport message disagrees with source")
var slug = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var eventType = regexp.MustCompile(`^[a-z][a-z0-9.-]{0,127}$`)

type Message struct {
	Version       int               `json:"version"`
	ID            string            `json:"id"`
	TenantID      string            `json:"tenant_id"`
	StreamID      string            `json:"stream_id"`
	Sequence      int64             `json:"sequence"`
	Type          string            `json:"type"`
	SchemaVersion int               `json:"schema_version"`
	Data          json.RawMessage   `json:"data"`
	Metadata      envelope.Metadata `json:"metadata"`
}

func TopicValid(topic string) bool {
	for _, t := range Topics {
		if t == topic {
			return true
		}
	}
	return false
}
func (m Message) Valid(topic string) bool {
	if !TopicValid(topic) || m.Version != 1 || m.ID != strings.ToLower(m.ID) || !envelope.ValidID(m.ID) || !slug.MatchString(m.TenantID) || m.Sequence < 1 || m.SchemaVersion < 1 || !eventType.MatchString(m.Type) || !m.Metadata.Valid() || len(m.StreamID) < 1 || len(m.StreamID) > 192 || len(m.Data) < 2 || len(m.Data) > MaxBytes || !json.Valid(m.Data) || bytes.TrimSpace(m.Data)[0] != '{' {
		return false
	}
	if topic != InventoryTopic && (!envelope.ValidID(m.StreamID) || m.StreamID != strings.ToLower(m.StreamID)) {
		return false
	}
	for _, r := range m.StreamID {
		if r < 33 || r > 126 {
			return false
		}
	}
	return true
}
func (m Message) Key() []byte { b, _ := json.Marshal([]string{m.TenantID, m.StreamID}); return b }
func Partition(key []byte) int32 {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int32(h.Sum32() % uint32(Partitions))
}
func InventoryStream(sku, warehouse string) string {
	if warehouse == "" {
		warehouse = "default"
	}
	return sku + "/" + warehouse
}
func Encode(topic string, m Message) ([]byte, error) {
	if !m.Valid(topic) {
		return nil, ErrInvalid
	}
	b, err := json.Marshal(m)
	if err != nil || len(b) > MaxBytes {
		return nil, ErrInvalid
	}
	return b, nil
}
func Decode(topic string, b []byte) (Message, error) {
	var m Message
	if len(b) > MaxBytes {
		return m, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF || !m.Valid(topic) {
		return m, ErrInvalid
	}
	return m, nil
}
func Canonical(b []byte) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
func Same(a, b []byte) bool {
	x, e := Canonical(a)
	y, f := Canonical(b)
	return e == nil && f == nil && bytes.Equal(x, y)
}
func Brokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	for _, p := range parts {
		if p == "" || strings.TrimSpace(p) != p || strings.ContainsAny(p, "/\\ \t\r\n") {
			return nil, ErrInvalid
		}
	}
	return parts, nil
}

type Publisher interface {
	Publish(context.Context, string, Message) error
}
type Handler interface {
	Apply(context.Context, string, Message) error
}
type HandlerFunc func(context.Context, string, Message) error

func (f HandlerFunc) Apply(ctx context.Context, topic string, m Message) error {
	return f(ctx, topic, m)
}

type Record struct {
	Topic      string
	Partition  int32
	Offset     int64
	Key, Value []byte
}
type Stager interface {
	Stage(context.Context, Record) error
}
