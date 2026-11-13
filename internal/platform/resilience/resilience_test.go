package resilience

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("resilience checkpoint did not converge")
}
func TestBulkheadBoundsCancellationAndGracefulDrain(t *testing.T) {
	b, e := NewBulkhead(1, 1)
	if e != nil {
		t.Fatal(e)
	}
	first, e := b.Acquire(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := b.Acquire(ctx); done <- e }()
	eventually(t, func() bool { return b.Stats().Queued == 1 })
	if _, e = b.Acquire(context.Background()); !errors.Is(e, ErrOverloaded) {
		t.Fatal(e)
	}
	cancel()
	if e = <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if s := b.Stats(); s.Active != 1 || s.Queued != 0 || s.Canceled != 1 || s.Rejected != 1 {
		t.Fatal(s)
	}
	queued := make(chan error, 1)
	go func() { _, e := b.Acquire(context.Background()); queued <- e }()
	eventually(t, func() bool { return b.Stats().Queued == 1 })
	b.BeginDrain()
	if e = <-queued; !errors.Is(e, ErrDraining) {
		t.Fatal(e)
	}
	if _, e = b.Acquire(context.Background()); !errors.Is(e, ErrDraining) {
		t.Fatal(e)
	}
	waiting, stop := context.WithCancel(context.Background())
	stop()
	if e = b.Drain(waiting); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	first.Release()
	first.Release()
	if e = b.Drain(context.Background()); e != nil || b.Stats().Active != 0 {
		t.Fatal(e, b.Stats())
	}
}
func TestBulkheadCancelAtGrantNeverLeaksCapacity(t *testing.T) {
	for i := 0; i < 100; i++ {
		b, _ := NewBulkhead(1, 1)
		first, _ := b.Acquire(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			p, e := b.Acquire(ctx)
			if p != nil {
				p.Release()
			}
			done <- e
		}()
		eventually(t, func() bool { return b.Stats().Queued == 1 })
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); first.Release() }()
		wg.Wait()
		<-done
		if s := b.Stats(); s.Active != 0 || s.Queued != 0 {
			t.Fatal("cancel/grant leaked a permit", s)
		}
	}
}
func TestCircuitEpochAndSingleHalfOpenProbe(t *testing.T) {
	now := time.Now()
	breaker, _ := NewBreaker(BreakerOptions{Failures: 2, OpenFor: time.Second, Now: func() time.Time { return now }})
	stale, _ := breaker.Allow()
	a, _ := breaker.Allow()
	a.Complete(Failed)
	b, _ := breaker.Allow()
	b.Complete(Failed)
	stale.Complete(Healthy)
	if s := breaker.Stats(); s.State != "open" || s.Opens != 1 {
		t.Fatal("old success closed a newer circuit", s)
	}
	if _, e := breaker.Allow(); !errors.Is(e, ErrCircuitOpen) {
		t.Fatal(e)
	}
	now = now.Add(2 * time.Second)
	probe, e := breaker.Allow()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = breaker.Allow(); !errors.Is(e, ErrCircuitOpen) {
		t.Fatal("unbounded recovery probes", e)
	}
	probe.Complete(Ignored)
	replacement, e := breaker.Allow()
	if e != nil {
		t.Fatal(e)
	}
	replacement.Complete(Healthy)
	if breaker.Stats().State != "closed" {
		t.Fatal(breaker.Stats())
	}
}
func TestGlobalRetryBudgetAndSafeOperationGate(t *testing.T) {
	p, _ := NewPolicy(4, 8, 3, time.Second, 100*time.Millisecond)
	p.Backoff = 0
	p.Breaker, _ = NewBreaker(BreakerOptions{Failures: 100, OpenFor: time.Second})
	unavailable := errors.New("transient dependency failure")
	var actual atomic.Int64
	for i := 0; i < 50; i++ {
		_, e := Do(context.Background(), p, ReadOnly, func(context.Context) (int, error) { actual.Add(1); return 0, unavailable })
		if !errors.Is(e, unavailable) {
			t.Fatal(e)
		}
	}
	s := p.Stats()
	if s.Budget.Originals != 50 || s.Physical != actual.Load() || s.Physical > 50+2+50/5 || s.Physical <= 50 {
		t.Fatal("retry amplification escaped budget", s)
	}
	safe, _ := NewPolicy(1, 0, 3, time.Second, 100*time.Millisecond)
	safe.Backoff = 0
	var calls int
	_, _ = Do(context.Background(), safe, NoRetry, func(context.Context) (int, error) { calls++; return 0, unavailable })
	if calls != 1 {
		t.Fatal("unsafe operation retried")
	}
	safe.Classify = func(error) Classification { return Classification{Outcome: Healthy} }
	_, _ = Do(context.Background(), safe, ReadOnly, func(context.Context) (int, error) { calls++; return 0, unavailable })
	if calls != 2 {
		t.Fatal("permanent rejection retried")
	}
}
func TestCoalescingSharesWorkWithoutFirstCallerCancellation(t *testing.T) {
	group, _ := NewGroup[int](2, time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	loaderCanceled := make(chan struct{})
	var calls atomic.Int64
	load := func(ctx context.Context) (int, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-ctx.Done():
			close(loaderCanceled)
			return 0, ctx.Err()
		case <-release:
			return 42, nil
		}
	}
	first, cancel := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	doneB := make(chan int, 1)
	go func() { _, _, e := group.Do(first, "key", load); doneA <- e }()
	<-entered
	go func() {
		v, _, e := group.Do(context.Background(), "key", load)
		if e != nil {
			doneB <- 0
		} else {
			doneB <- v
		}
	}()
	eventually(t, func() bool { return group.Stats().Shared == 1 })
	cancel()
	if e := <-doneA; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	select {
	case <-loaderCanceled:
		t.Fatal("first caller canceled another waiter's load")
	default:
	}
	close(release)
	if v := <-doneB; v != 42 || calls.Load() != 1 {
		t.Fatal(v, calls.Load())
	}
	if e := group.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func TestAbandonedAndInvalidatedLoadsRemainWithinConcurrencyBound(t *testing.T) {
	group, _ := NewGroup[int](1, time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, _, e := group.Do(ctx, "old", func(context.Context) (int, error) { close(entered); <-release; return 1, nil })
		done <- e
	}()
	<-entered
	cancel()
	<-done
	if _, _, e := group.Do(context.Background(), "other", func(context.Context) (int, error) { return 2, nil }); !errors.Is(e, ErrOverloaded) {
		t.Fatal("abandoned goroutine escaped active bound", e)
	}
	close(release)
	eventually(t, func() bool { return group.Stats().Active == 0 })
	group2, _ := NewGroup[int](2, time.Second)
	blocked, finish := make(chan struct{}), make(chan struct{})
	old := make(chan error, 1)
	go func() {
		_, _, e := group2.Do(context.Background(), "key", func(context.Context) (int, error) { close(blocked); <-finish; return 1, nil })
		old <- e
	}()
	<-blocked
	group2.Invalidate("key")
	value, _, e := group2.Do(context.Background(), "key", func(context.Context) (int, error) { return 2, nil })
	if e != nil || value != 2 {
		t.Fatal(value, e)
	}
	close(finish)
	if e = <-old; !errors.Is(e, ErrInvalidated) {
		t.Fatal("obsolete load returned as fresh", e)
	}
	if e = group.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = group2.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}
