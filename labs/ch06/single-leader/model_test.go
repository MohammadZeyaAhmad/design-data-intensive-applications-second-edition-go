package singleleader

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func TestDurabilityAgainstPromotionSchedules(t *testing.T) {
	for _, mode := range []Mode{Async, RemoteFlush, RemoteApply} {
		for seed := int64(0); seed < 64; seed++ {
			p, _ := New(mode)
			rng := rand.New(rand.NewSource(seed))
			acked := map[int]Entry{}
			for i := range 40 {
				p.Transport = rng.Intn(3) > 0
				p.Replay = rng.Intn(2) > 0
				e := Entry{fmt.Sprint(i), int64(rng.Intn(100) + 1)}
				lsn, ack, err := p.Write(1, e)
				if err != nil {
					t.Fatal(err)
				}
				if ack {
					acked[lsn] = e
				}
				if p.Follower.Applied > len(p.Follower.Log) || len(p.Follower.Log) > len(p.Leader.Log) {
					t.Fatal("stage order")
				}
				if p.Acknowledgable(lsn) && mode == RemoteApply && p.Follower.Quantity() != p.Leader.Quantity() {
					t.Fatal("apply acknowledged stale state")
				}
			}
			if _, err := p.Promote(false); err == nil {
				t.Fatal("unfenced promotion accepted")
			}
			if _, err := p.Promote(true); err != nil {
				t.Fatal(err)
			}
			if mode != Async {
				for lsn, e := range acked {
					if lsn > len(p.Leader.Log) || p.Leader.Log[lsn-1] != e {
						t.Fatalf("%s seed %d lost ack", mode, seed)
					}
				}
			}
			if _, _, err := p.Write(1, Entry{"stale", 1}); !errors.Is(err, ErrFenced) {
				t.Fatal("old epoch admitted")
			}
		}
	}
}
func TestBootstrapSuffixRetentionAndIdempotency(t *testing.T) {
	p, _ := New(Async)
	_, _, _ = p.Write(1, Entry{"a", 1})
	p.Bootstrap()
	p.Transport = false
	_, _, _ = p.Write(1, Entry{"b", 2})
	p.Floor = 2
	p.Transport = true
	if !errors.Is(p.Step(), ErrSnapshotRequired) {
		t.Fatal("gap silently repaired")
	}
	p.Bootstrap()
	_, ack, err := p.Write(1, Entry{"b", 2})
	if err != nil || !ack || len(p.Leader.Log) != 2 {
		t.Fatal("duplicate not resolved")
	}
	if _, _, err = p.Write(1, Entry{"b", 3}); err == nil {
		t.Fatal("intent conflict ignored")
	}
	if p.Follower.Quantity() != 3 {
		t.Fatal("snapshot omitted state")
	}
}
func TestCounterexamples(t *testing.T) {
	if _, err := Demonstrate(); err != nil {
		t.Fatal(err)
	}
}

func TestFailureDomainsDoNotAssumeIndependentCopies(t *testing.T) {
	cases := domainExamples()
	want := []int{0, 1, 0, 1}
	for i, c := range cases {
		if c.SurvivingDurableCopies != want[i] {
			t.Fatalf("placement %s: %+v", c.Placement, c)
		}
	}
}
