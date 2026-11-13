// Package actors is an inventory-shaped actor laboratory, not a durable queue
// or a distributed ownership service.
package actors

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"unicode/utf8"
)

const MaxMessageBytes = 4096

var (
	ErrInvalid          = errors.New("invalid actor message")
	ErrUnsupported      = errors.New("unsupported actor message version or operation")
	ErrInsufficient     = errors.New("insufficient actor stock")
	ErrIdentityConflict = errors.New("message identity has another payload")
)
var slug = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)
var skuPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type Key struct {
	Tenant    string `json:"tenant"`
	Warehouse string `json:"warehouse,omitempty"`
	SKU       string `json:"sku"`
}

func (k Key) Normalize() Key {
	if k.Warehouse == "default" {
		k.Warehouse = ""
	}
	return k
}
func (k Key) Valid() bool {
	return slug.MatchString(k.Tenant) && (k.Warehouse == "" || slug.MatchString(k.Warehouse)) && skuPattern.MatchString(k.SKU)
}
func (k Key) String() string { k = k.Normalize(); return k.Tenant + "/" + k.Warehouse + "/" + k.SKU }

type Message struct {
	ID       string
	Key      Key
	Kind     string
	Writer   int
	Quantity int64
	Note     string
	Raw      []byte
	Hash     [32]byte
}
type envelope struct {
	Format int             `json:"format"`
	Writer int             `json:"writer_revision"`
	ID     string          `json:"id"`
	Key    Key             `json:"key"`
	Kind   string          `json:"kind"`
	Data   json.RawMessage `json:"data"`
}
type payloadV1 struct {
	Quantity int64 `json:"quantity,omitempty"`
}
type payloadV2 struct {
	Quantity int64  `json:"quantity,omitempty"`
	Note     string `json:"note,omitempty"`
}

// Encode uses distinct typed writer profiles. Reader 1 deliberately lacks the
// optional note, but both readers understand the same versioned operations.
func Encode(writer int, id string, key Key, kind string, quantity int64, note string) ([]byte, error) {
	var body []byte
	var e error
	switch writer {
	case 1:
		body, e = json.Marshal(payloadV1{Quantity: quantity})
	case 2:
		body, e = json.Marshal(payloadV2{Quantity: quantity, Note: note})
	default:
		return nil, ErrUnsupported
	}
	if e != nil {
		return nil, e
	}
	raw, e := json.Marshal(envelope{Format: 1, Writer: writer, ID: id, Key: key.Normalize(), Kind: kind, Data: body})
	if e != nil {
		return nil, e
	}
	if _, e = Decode(raw, writer); e != nil {
		return nil, e
	}
	return raw, nil
}
func Decode(raw []byte, reader int) (Message, error) {
	if reader < 1 || reader > 2 {
		return Message{}, ErrUnsupported
	}
	if len(raw) == 0 || len(raw) > MaxMessageBytes || !utf8.Valid(raw) {
		return Message{}, ErrInvalid
	}
	if e := boundedJSON(raw); e != nil {
		return Message{}, e
	}
	var wire envelope
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(&wire); e != nil {
		return Message{}, ErrInvalid
	}
	if wire.Format != 1 || wire.Writer < 1 || wire.Writer > 2 {
		return Message{}, ErrUnsupported
	}
	key := wire.Key.Normalize()
	if !key.Valid() || !idPattern.MatchString(wire.ID) {
		return Message{}, ErrInvalid
	}
	var current payloadV2
	if e := json.Unmarshal(wire.Data, &current); e != nil || len(current.Note) > 128 {
		return Message{}, ErrInvalid
	}
	switch wire.Kind {
	case "receive.v1", "reserve.v1":
		if current.Quantity < 1 || current.Quantity > 1_000_000_000 {
			return Message{}, ErrInvalid
		}
	case "get.v1":
		if current.Quantity != 0 {
			return Message{}, ErrInvalid
		}
	default:
		return Message{}, ErrUnsupported
	}
	// Bind the complete canonical payload, including unknown annotations. A
	// typed old relay cannot silently replace a durable receipt's original body.
	var generic map[string]any
	pd := json.NewDecoder(bytes.NewReader(wire.Data))
	pd.UseNumber()
	if e := pd.Decode(&generic); e != nil || generic == nil {
		return Message{}, ErrInvalid
	}
	canonical, e := json.Marshal(struct {
		Key  Key
		Kind string
		Data map[string]any
	}{key, wire.Kind, generic})
	if e != nil {
		return Message{}, ErrInvalid
	}
	msg := Message{ID: wire.ID, Key: key, Kind: wire.Kind, Writer: wire.Writer, Quantity: current.Quantity, Raw: append([]byte(nil), raw...), Hash: sha256.Sum256(canonical)}
	if reader == 1 {
		var old payloadV1
		if e = json.Unmarshal(wire.Data, &old); e != nil {
			return Message{}, ErrInvalid
		}
		msg.Quantity = old.Quantity
	} else {
		msg.Note = current.Note
	}
	return msg, nil
}
func PreserveRelay(raw []byte) ([]byte, error) {
	if _, e := Decode(raw, 1); e != nil {
		return nil, e
	}
	return append([]byte(nil), raw...), nil
}
func TypedOldRelay(raw []byte) ([]byte, error) {
	m, e := Decode(raw, 1)
	if e != nil {
		return nil, e
	}
	return Encode(1, m.ID, m.Key, m.Kind, m.Quantity, "")
}
func boundedJSON(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	tokens := 0
	var visit func(int) error
	visit = func(depth int) error {
		if depth > 6 {
			return ErrInvalid
		}
		tokens++
		if tokens > 128 {
			return ErrInvalid
		}
		token, e := d.Token()
		if e != nil {
			return ErrInvalid
		}
		switch v := token.(type) {
		case string:
			if len(v) > 512 {
				return ErrInvalid
			}
		case json.Delim:
			switch v {
			case '{':
				keys := map[string]bool{}
				for d.More() {
					key, e := d.Token()
					name, ok := key.(string)
					if e != nil || !ok || len(name) > 64 || keys[name] || len(keys) >= 16 {
						return ErrInvalid
					}
					keys[name] = true
					if e = visit(depth + 1); e != nil {
						return e
					}
				}
				end, e := d.Token()
				if e != nil || end != json.Delim('}') {
					return ErrInvalid
				}
			case '[':
				n := 0
				for d.More() {
					n++
					if n > 8 {
						return ErrInvalid
					}
					if e = visit(depth + 1); e != nil {
						return e
					}
				}
				end, e := d.Token()
				if e != nil || end != json.Delim(']') {
					return ErrInvalid
				}
			default:
				return ErrInvalid
			}
		}
		return nil
	}
	if e := visit(0); e != nil {
		return e
	}
	if _, e := d.Token(); !errors.Is(e, io.EOF) {
		return fmt.Errorf("%w: trailing JSON", ErrInvalid)
	}
	return nil
}
