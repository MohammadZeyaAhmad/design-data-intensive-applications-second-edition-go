// Package singleleader models WAL durability separately from replica visibility.
// It is a finite teaching model, not a consensus protocol or a database engine.
package singleleader

import "errors"

type Mode string

const (
	Async       Mode = "async"
	RemoteFlush Mode = "remote_flush"
	RemoteApply Mode = "remote_apply"
)

type Entry struct {
	Key      string
	Quantity int64
}
type Node struct {
	Log      []Entry
	Applied  int
	Epoch    int64
	Writable bool
}
type Pair struct {
	Leader, Follower  Node
	Mode              Mode
	Transport, Replay bool
	Floor             int
}

var ErrFenced = errors.New("writer fenced")
var ErrSnapshotRequired = errors.New("retained suffix cannot fill follower gap")

func New(mode Mode) (*Pair, error) {
	if mode != Async && mode != RemoteFlush && mode != RemoteApply {
		return nil, errors.New("unknown durability mode")
	}
	return &Pair{Leader: Node{Epoch: 1, Writable: true}, Follower: Node{Epoch: 1}, Mode: mode, Transport: true, Replay: true}, nil
}

// Write is locally durable before a reply can be lost or a synchronous wait
// begins. Repeating an identical intent resolves uncertainty without a new log.
func (p *Pair) Write(epoch int64, e Entry) (int, bool, error) {
	if !p.Leader.Writable || epoch != p.Leader.Epoch {
		return 0, false, ErrFenced
	}
	if e.Key == "" || e.Quantity <= 0 || e.Quantity > 1000000000 {
		return 0, false, errors.New("bounded command required")
	}
	for i, prior := range p.Leader.Log {
		if prior.Key == e.Key {
			if prior != e {
				return 0, false, errors.New("intent conflict")
			}
			return i + 1, p.Acknowledgable(i + 1), nil
		}
	}
	if len(p.Leader.Log) >= 4096 {
		return 0, false, errors.New("model log bound reached")
	}
	p.Leader.Log = append(p.Leader.Log, e)
	p.Leader.Applied = len(p.Leader.Log)
	if err := p.Step(); err != nil {
		return len(p.Leader.Log), false, err
	}
	return len(p.Leader.Log), p.Acknowledgable(len(p.Leader.Log)), nil
}
func (p *Pair) Step() error {
	if p.Transport {
		if len(p.Follower.Log) < p.Floor {
			return ErrSnapshotRequired
		}
		p.Follower.Log = append(p.Follower.Log, p.Leader.Log[len(p.Follower.Log):]...)
	}
	if p.Replay {
		p.Follower.Applied = len(p.Follower.Log)
	}
	return nil
}
func (p *Pair) Acknowledgable(lsn int) bool {
	if lsn < 1 || lsn > len(p.Leader.Log) {
		return false
	}
	switch p.Mode {
	case Async:
		return true
	case RemoteFlush:
		return len(p.Follower.Log) >= lsn
	case RemoteApply:
		return p.Follower.Applied >= lsn
	}
	return false
}
func (n Node) Quantity() int64 {
	var q int64
	for _, e := range n.Log[:n.Applied] {
		q += e.Quantity
	}
	return q
}
func (p *Pair) Bootstrap() {
	p.Follower = Node{Log: append([]Entry(nil), p.Leader.Log...), Applied: p.Leader.Applied, Epoch: p.Leader.Epoch}
}

// Promote requires a confirmed external fence. A copied epoch alone cannot
// stop the old database accepting writes on a different branch.
func (p *Pair) Promote(oldPrimaryStopped bool) (Node, error) {
	if !oldPrimaryStopped {
		return Node{}, errors.New("old primary must be fenced first")
	}
	p.Leader.Writable = false
	old := p.Leader
	p.Leader = p.Follower
	p.Leader.Epoch++
	p.Leader.Writable = true
	p.Leader.Applied = len(p.Leader.Log)
	p.Follower = Node{Epoch: p.Leader.Epoch}
	p.Floor = 0
	return old, nil
}

type ModelReport struct {
	FailureDomains              []DomainEvidence `json:"failure_domains"`
	AsyncLostAcknowledgments    int              `json:"async_lost_acknowledgments"`
	FlushAcknowledgedWhileStale bool             `json:"flush_acknowledged_while_stale"`
	ApplyWithheldWhileStale     bool             `json:"apply_withheld_while_stale"`
	UnsafeSplitBrainQuantities  [2]int64         `json:"unsafe_split_brain_quantities"`
	Assumptions                 []string         `json:"assumptions"`
}

func Demonstrate() (ModelReport, error) {
	r := ModelReport{Assumptions: []string{"one leader plus one follower; memory objects represent durable logs", "remote flush survives primary failure only while that follower survives", "two nodes in one zone share a failure domain; separate zones still share a region", "latency and cloud failure independence are not inferred from this model"}}
	r.FailureDomains = domainExamples()
	p, _ := New(Async)
	p.Transport = false
	_, ack, err := p.Write(1, Entry{"lost", 1})
	if err != nil || !ack {
		return r, errors.New("async counterexample missing")
	}
	if _, err = p.Promote(true); err != nil {
		return r, err
	}
	if p.Leader.Quantity() == 0 {
		r.AsyncLostAcknowledgments = 1
	}
	p, _ = New(RemoteFlush)
	p.Replay = false
	_, ack, err = p.Write(1, Entry{"flushed", 1})
	if err != nil {
		return r, err
	}
	r.FlushAcknowledgedWhileStale = ack && p.Follower.Quantity() == 0
	p, _ = New(RemoteApply)
	p.Replay = false
	_, ack, err = p.Write(1, Entry{"apply", 1})
	if err != nil {
		return r, err
	}
	r.ApplyWithheldWhileStale = !ack && p.Follower.Quantity() == 0
	// Deliberately bypass Promote's fence to show independently copied epochs
	// do not establish exclusive authority across a partition.
	a, _ := New(Async)
	b, _ := New(Async)
	a.Transport = false
	b.Transport = false
	_, _, _ = a.Write(1, Entry{"same-intent", 1})
	_, _, _ = b.Write(1, Entry{"same-intent", 2})
	r.UnsafeSplitBrainQuantities = [2]int64{a.Leader.Quantity(), b.Leader.Quantity()}
	if r.AsyncLostAcknowledgments != 1 || !r.FlushAcknowledgedWhileStale || !r.ApplyWithheldWhileStale {
		return r, errors.New("model invariants failed")
	}
	return r, nil
}
