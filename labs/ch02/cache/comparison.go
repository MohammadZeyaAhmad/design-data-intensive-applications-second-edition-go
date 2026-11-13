// Package cache contrasts acknowledgment and ordering in a finite write-behind
// model. It is a teaching counterexample, never an authoritative Atlas adapter.
package cache

import (
	"errors"
)

type Value struct {
	Version int64 `json:"version"`
	Price   int64 `json:"price_minor"`
}
type Origin struct{ Value Value }

func (o *Origin) Apply(value Value, fenced bool) bool {
	if fenced && value.Version <= o.Value.Version {
		return false
	}
	o.Value = value
	return true
}

type Behind struct {
	origin   *Origin
	capacity int
	pending  []Value
	cached   Value
}

func NewBehind(origin *Origin, capacity int) (*Behind, error) {
	if origin == nil || capacity < 1 || capacity > 16 {
		return nil, errors.New("invalid write-behind bounds")
	}
	return &Behind{origin: origin, capacity: capacity, cached: origin.Value}, nil
}
func (b *Behind) Write(value Value) error {
	if len(b.pending) == b.capacity {
		return errors.New("write-behind queue full")
	}
	b.pending = append(b.pending, value)
	b.cached = value
	return nil
}
func (b *Behind) FlushNewestFirst(fenced bool) {
	for len(b.pending) > 0 {
		i := len(b.pending) - 1
		b.origin.Apply(b.pending[i], fenced)
		b.pending = b.pending[:i]
	}
}
func (b *Behind) Crash() { b.pending = nil; b.cached = Value{} }

type Report struct {
	Model                 string `json:"model"`
	Acknowledged          Value  `json:"acknowledged_before_crash"`
	AfterCrash            Value  `json:"origin_after_crash"`
	Unfenced              Value  `json:"unfenced_reverse_flush"`
	Fenced                Value  `json:"fenced_reverse_flush"`
	QueueRejected         bool   `json:"full_queue_rejected_before_ack"`
	LostAcknowledgedWrite bool   `json:"lost_acknowledged_write"`
	ReorderedWrite        bool   `json:"reordered_write"`
}

func Run() (Report, error) {
	original := Value{Version: 1, Price: 1000}
	v2 := Value{Version: 2, Price: 2000}
	v3 := Value{Version: 3, Price: 3000}
	origin := &Origin{Value: original}
	behind, _ := NewBehind(origin, 2)
	if err := behind.Write(v2); err != nil {
		return Report{}, err
	}
	ack := behind.cached
	behind.Crash()
	report := Report{Model: "finite volatile write-behind counterexample; no disk or process-durability claim", Acknowledged: ack, AfterCrash: origin.Value, LostAcknowledgedWrite: ack != origin.Value}
	for _, fenced := range []bool{false, true} {
		target := &Origin{Value: original}
		queue, _ := NewBehind(target, 2)
		if err := queue.Write(v2); err != nil {
			return report, err
		}
		if err := queue.Write(v3); err != nil {
			return report, err
		}
		report.QueueRejected = queue.Write(Value{Version: 4, Price: 4000}) != nil
		queue.FlushNewestFirst(fenced)
		if fenced {
			report.Fenced = target.Value
		} else {
			report.Unfenced = target.Value
		}
	}
	report.ReorderedWrite = report.Unfenced.Version < report.Fenced.Version
	if !report.LostAcknowledgedWrite || !report.ReorderedWrite || !report.QueueRejected || report.Fenced != v3 {
		return report, errors.New("expected cache counterexample missing")
	}
	return report, nil
}
