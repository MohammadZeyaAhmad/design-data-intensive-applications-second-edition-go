package resilience

import (
	"errors"
	"sync"
	"time"
)

type Outcome uint8

const (
	Healthy Outcome = iota
	Failed
	Ignored
)

type BreakerOptions struct {
	Failures int
	OpenFor  time.Duration
	Now      func() time.Time
}
type BreakerStats struct {
	State    string `json:"state"`
	Failures int    `json:"consecutive_failures"`
	Rejected int64  `json:"rejected"`
	Opens    int64  `json:"opens"`
	Probes   int64  `json:"probes"`
	Epoch    uint64 `json:"epoch"`
}
type Breaker struct {
	mu                      sync.Mutex
	options                 BreakerOptions
	state                   string
	failures                int
	until                   time.Time
	probe                   bool
	epoch                   uint64
	rejected, opens, probes int64
}
type Attempt struct {
	once    sync.Once
	breaker *Breaker
	epoch   uint64
}

func NewBreaker(o BreakerOptions) (*Breaker, error) {
	if o.Failures < 1 || o.Failures > 100 || o.OpenFor < time.Millisecond || o.OpenFor > time.Minute {
		return nil, errors.New("invalid circuit breaker options")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Breaker{options: o, state: "closed"}, nil
}
func (b *Breaker) Allow() (*Attempt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == "open" {
		if b.options.Now().Before(b.until) {
			b.rejected++
			return nil, ErrCircuitOpen
		}
		b.state = "half_open"
		b.probe = false
		b.epoch++
	}
	if b.state == "half_open" {
		if b.probe {
			b.rejected++
			return nil, ErrCircuitOpen
		}
		b.probe = true
		b.probes++
	}
	return &Attempt{breaker: b, epoch: b.epoch}, nil
}
func (a *Attempt) Complete(outcome Outcome) {
	if a == nil {
		return
	}
	a.once.Do(func() {
		b := a.breaker
		b.mu.Lock()
		defer b.mu.Unlock()
		if a.epoch != b.epoch {
			return
		}
		if outcome == Ignored {
			if b.state == "half_open" {
				b.probe = false
			}
			return
		}
		if outcome == Healthy {
			b.failures = 0
			if b.state == "half_open" {
				b.state = "closed"
				b.probe = false
				b.epoch++
			}
			return
		}
		b.failures++
		if b.state == "half_open" || b.failures >= b.options.Failures {
			b.state = "open"
			b.until = b.options.Now().Add(b.options.OpenFor)
			b.probe = false
			b.epoch++
			b.opens++
		}
	})
}
func (b *Breaker) Stats() BreakerStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BreakerStats{State: b.state, Failures: b.failures, Rejected: b.rejected, Opens: b.opens, Probes: b.probes, Epoch: b.epoch}
}
