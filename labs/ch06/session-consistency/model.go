// Package sessionconsistency models replica replay and verifies the real Atlas
// HTTP contract with disposable PostgreSQL primary and standby processes.
package sessionconsistency

import "fmt"

type Observation struct {
	Name   string `json:"name"`
	Before string `json:"before"`
	After  string `json:"after"`
}
type Vector map[string]uint64

func (v Vector) Observe(shard string, position uint64) {
	if position > v[shard] {
		v[shard] = position
	}
}
func (v Vector) Satisfied(replay Vector) bool {
	for s, p := range v {
		if replay[s] < p {
			return false
		}
	}
	return true
}

// Model deliberately separates independently ordered shard positions. There
// is no universal LSN and no inferred causal edge from comparing two numbers.
func Model() ([]Observation, error) {
	v := Vector{}
	v.Observe("inventory", 2)
	if v.Satisfied(Vector{"inventory": 1}) {
		return nil, fmt.Errorf("read-your-writes fence accepted stale replica")
	}
	v.Observe("inventory", 1)
	if v["inventory"] != 2 {
		return nil, fmt.Errorf("session watermark regressed")
	}
	// A debit at A1 precedes credit B1, and B's row explicitly carries A1.
	parent := Vector{"a": 1}
	child := Vector{"b": 1}
	child.Observe("a", parent["a"])
	if child.Satisfied(Vector{"a": 0, "b": 1}) {
		return nil, fmt.Errorf("child missing parent")
	}
	if !child.Satisfied(Vector{"a": 1, "b": 1}) {
		return nil, fmt.Errorf("valid causal prefix rejected")
	}
	// A1+B0 satisfies its own fences and the causal prefix (only the debit
	// is visible), but cannot be an atomic cross-shard transfer snapshot.
	partial := Vector{"a": 1, "b": 0}
	if !partial.Satisfied(Vector{"a": 1, "b": 0}) {
		return nil, fmt.Errorf("partial cut unexpectedly rejected")
	}
	return []Observation{
		{"stale / read-your-writes", "acknowledged version 2; replica returns version 1", "require inventory>=2, wait or primary fallback"},
		{"monotonic", "alternating replicas return 2 then 1", "max session watermark remains 2; exclude replay 1"},
		{"consistent prefix", "A0=10 and dependent B1=10 imply sum 20", "B1 carries A1 dependency; re-read A>=1 gives sum 10"},
		{"cross-shard atomicity limit", "A1=0 and B0=0 satisfy causal prefix and per-shard fences", "sum 0 remains possible; a distributed atomic snapshot needs another protocol"},
	}, nil
}
