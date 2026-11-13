package resilience

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Budget permits at most burst + floor(originals*percent/100) extra calls over
// its lifetime. Rejected admission does not mint retry credit.
type Budget struct {
	mu                         sync.Mutex
	percent, burst             int64
	credit                     int64
	originals, retries, denied int64
}
type BudgetStats struct {
	Originals int64 `json:"originals"`
	Retries   int64 `json:"retries"`
	Denied    int64 `json:"denied"`
	Percent   int64 `json:"percent"`
	Burst     int64 `json:"burst"`
}

func NewBudget(percent, burst int) (*Budget, error) {
	if percent < 0 || percent > 100 || burst < 0 || burst > 100 {
		return nil, errors.New("invalid retry budget")
	}
	return &Budget{percent: int64(percent), burst: int64(burst), credit: int64(burst) * 100}, nil
}
func (b *Budget) Original() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.originals++
	b.credit += b.percent
	if b.credit > b.burst*100 {
		b.credit = b.burst * 100
	}
}
func (b *Budget) Take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.credit < 100 {
		b.denied++
		return false
	}
	b.credit -= 100
	b.retries++
	return true
}
func (b *Budget) Stats() BudgetStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BudgetStats{Originals: b.originals, Retries: b.retries, Denied: b.denied, Percent: b.percent, Burst: b.burst}
}

type Safety uint8

const (
	NoRetry Safety = iota
	ReadOnly
	Idempotent
)

type Classification struct {
	Retry   bool
	Outcome Outcome
}
type Policy struct {
	Bulkhead                              *Bulkhead
	Breaker                               *Breaker
	Budget                                *Budget
	TotalTimeout, AttemptTimeout, Backoff time.Duration
	Attempts                              int
	Classify                              func(error) Classification
	calls, physical, rejected             atomic.Int64
}
type PolicyStats struct {
	Calls    int64         `json:"calls"`
	Physical int64         `json:"physical"`
	Rejected int64         `json:"rejected"`
	Bulkhead BulkheadStats `json:"bulkhead"`
	Breaker  BreakerStats  `json:"breaker"`
	Budget   BudgetStats   `json:"retry_budget"`
}

func NewPolicy(active, queued, attempts int, total, perAttempt time.Duration) (*Policy, error) {
	if attempts < 1 || attempts > 3 || total < time.Millisecond || total > 30*time.Second || perAttempt < time.Millisecond || perAttempt > total {
		return nil, errors.New("invalid dependency policy")
	}
	b, e := NewBulkhead(active, queued)
	if e != nil {
		return nil, e
	}
	c, e := NewBreaker(BreakerOptions{Failures: 4, OpenFor: 500 * time.Millisecond})
	if e != nil {
		return nil, e
	}
	budget, e := NewBudget(20, 2)
	if e != nil {
		return nil, e
	}
	return &Policy{Bulkhead: b, Breaker: c, Budget: budget, TotalTimeout: total, AttemptTimeout: perAttempt, Attempts: attempts, Backoff: 10 * time.Millisecond, Classify: func(e error) Classification {
		if e == nil {
			return Classification{Outcome: Healthy}
		}
		return Classification{Retry: true, Outcome: Failed}
	}}, nil
}
func Do[T any](parent context.Context, p *Policy, safety Safety, call func(context.Context) (T, error)) (out T, err error) {
	ctx, cancel := context.WithTimeout(parent, p.TotalTimeout)
	defer cancel()
	p.calls.Add(1)
	for i := 0; i < p.Attempts; i++ {
		if e := ctx.Err(); e != nil {
			return out, e
		}
		breaker, e := p.Breaker.Allow()
		if e != nil {
			p.rejected.Add(1)
			if err != nil {
				return out, err
			}
			return out, e
		}
		// Also release probe ownership if configuration/classification panics.
		defer breaker.Complete(Ignored)
		permit, e := p.Bulkhead.Acquire(ctx)
		if e != nil {
			breaker.Complete(Ignored)
			p.rejected.Add(1)
			return out, e
		}
		if e = ctx.Err(); e != nil {
			permit.Release()
			breaker.Complete(Ignored)
			return out, e
		}
		if i == 0 {
			p.Budget.Original()
		}
		p.physical.Add(1)
		attempt, stop := context.WithTimeout(ctx, p.AttemptTimeout)
		// A panic must still release the scarce permit and half-open probe.
		func() {
			defer permit.Release()
			defer stop()
			defer func() {
				if recovered := recover(); recovered != nil {
					breaker.Complete(Ignored)
					panic(recovered)
				}
			}()
			out, err = call(attempt)
		}()
		class := p.Classify(err)
		if ctx.Err() != nil {
			class = Classification{Outcome: Ignored}
		}
		breaker.Complete(class.Outcome)
		if err == nil {
			return out, nil
		}
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		if (safety != ReadOnly && safety != Idempotent) || !class.Retry || i+1 == p.Attempts {
			return out, err
		}
		if !p.Budget.Take() {
			return out, err
		}
		delay := p.Backoff * time.Duration(1<<i)
		if delay > 100*time.Millisecond {
			delay = 100 * time.Millisecond
		}
		delay += time.Duration(rand.Int64N(int64(p.Backoff) + 1))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return out, ctx.Err()
		case <-timer.C:
		}
	}
	return out, err
}
func (p *Policy) Stats() PolicyStats {
	return PolicyStats{Calls: p.calls.Load(), Physical: p.physical.Load(), Rejected: p.rejected.Load(), Bulkhead: p.Bulkhead.Stats(), Breaker: p.Breaker.Stats(), Budget: p.Budget.Stats()}
}
