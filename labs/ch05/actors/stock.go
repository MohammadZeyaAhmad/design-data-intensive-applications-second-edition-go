package actors

import (
	"context"
	"math"
)

type State struct {
	Key      Key   `json:"key"`
	OnHand   int64 `json:"on_hand"`
	Reserved int64 `json:"reserved"`
	Version  int64 `json:"version"`
}
type Reply struct {
	State    State  `json:"state"`
	Replayed bool   `json:"replayed"`
	Note     string `json:"note,omitempty"`
	Ref      Ref    `json:"ref"`
}

func Evolve(s State, m Message) (State, error) {
	if s.Key.Normalize() != m.Key || s.OnHand < 0 || s.Reserved < 0 || s.Reserved > s.OnHand || s.Version < 0 {
		return s, ErrInvalid
	}
	if m.Kind == "get.v1" {
		if m.Quantity != 0 {
			return s, ErrInvalid
		}
		return s, nil
	}
	if m.Quantity < 1 || m.Quantity > 1_000_000_000 {
		return s, ErrInvalid
	}
	if s.Version == math.MaxInt64 {
		return s, ErrInvalid
	}
	switch m.Kind {
	case "receive.v1":
		if m.Quantity > math.MaxInt64-s.OnHand {
			return s, ErrInvalid
		}
		s.OnHand += m.Quantity
	case "reserve.v1":
		if m.Quantity > s.OnHand-s.Reserved {
			return s, ErrInsufficient
		}
		s.Reserved += m.Quantity
	default:
		return s, ErrUnsupported
	}
	s.Version++
	return s, nil
}

type Processor func(context.Context, Message) (Reply, error)
type Factory func(Key) Processor
type receipt struct {
	hash  [32]byte
	reply Reply
}

func MemoryFactory(retained int) Factory {
	return func(key Key) Processor {
		state := State{Key: key}
		receipts := map[string]receipt{}
		order := []string{}
		return func(ctx context.Context, m Message) (Reply, error) {
			if e := ctx.Err(); e != nil {
				return Reply{}, e
			}
			if m.Kind != "get.v1" {
				if saved, ok := receipts[m.ID]; ok {
					if saved.hash != m.Hash {
						return Reply{}, ErrIdentityConflict
					}
					out := saved.reply
					out.Replayed = true
					return out, nil
				}
			}
			next, e := Evolve(state, m)
			if e != nil {
				return Reply{}, e
			}
			state = next
			out := Reply{State: state, Note: m.Note}
			if m.Kind != "get.v1" {
				if len(order) == retained {
					delete(receipts, order[0])
					order = order[1:]
				}
				receipts[m.ID] = receipt{hash: m.Hash, reply: out}
				order = append(order, m.ID)
			}
			return out, nil
		}
	}
}
