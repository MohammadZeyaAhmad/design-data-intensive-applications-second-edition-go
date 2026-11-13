package actors

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func runtimeFixture(t *testing.T, o Options) *Runtime {
	t.Helper()
	r, e := New(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e = r.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	return r
}

var testKey = Key{Tenant: "acme", SKU: "BOOK"}

func wire(t *testing.T, id, kind string, key Key, q int64) []byte {
	t.Helper()
	b, e := Encode(2, id, key, kind, q, "annotation")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func ask(t *testing.T, r *Runtime, ref Ref, id, kind string, q int64) Reply {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out, e := r.Ask(ctx, ref, wire(t, id, kind, ref.Key, q))
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("controlled actor checkpoint not reached")
	}
}
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actor condition did not converge")
}
func TestMailboxBackpressureIsolationAndImmutableAdmission(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	o := DefaultOptions()
	o.Mailbox = 1
	o.Hooks.Before = func(ctx context.Context, m Message) error {
		if m.ID == "block" {
			close(entered)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
		}
		return nil
	}
	r := runtimeFixture(t, o)
	ref, e := r.Resolve(testKey)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Tell(context.Background(), ref, wire(t, "block", "receive.v1", testKey, 1)); e != nil {
		t.Fatal(e)
	}
	await(t, entered)
	original := wire(t, "queued", "receive.v1", testKey, 2)
	if _, e = r.Tell(context.Background(), ref, original); e != nil {
		t.Fatal(e)
	}
	copy(original, bytes.Repeat([]byte("x"), len(original)))
	if _, e = r.Tell(context.Background(), ref, wire(t, "overflow", "receive.v1", testKey, 9)); !errors.Is(e, ErrBackpressure) {
		t.Fatal("unbounded mailbox:", e)
	}
	other, e := r.Resolve(Key{Tenant: "other", SKU: "BOOK"})
	if e != nil {
		t.Fatal(e)
	}
	if out := ask(t, r, other, "independent", "receive.v1", 7); out.State.OnHand != 7 {
		t.Fatal("unrelated actor blocked")
	}
	close(release)
	eventually(t, func() bool { return r.Stats().Processed == 3 })
	if out := ask(t, r, ref, "read", "get.v1", 0); out.State.OnHand != 3 {
		t.Fatalf("queued bytes shared caller memory: %+v", out)
	}
	if r.Stats().Backpressure != 1 {
		t.Fatal(r.Stats())
	}
}
func TestSerializedReservationsNeverOversell(t *testing.T) {
	o := DefaultOptions()
	o.Mailbox = 128
	r := runtimeFixture(t, o)
	ref, e := r.Resolve(testKey)
	if e != nil {
		t.Fatal(e)
	}
	ask(t, r, ref, "seed", "receive.v1", 10)
	errs := make(chan error, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		raw := wire(t, fmt.Sprintf("reserve-%d", i), "reserve.v1", testKey, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, e := r.Ask(ctx, ref, raw)
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	accepted, insufficient := 0, 0
	for e := range errs {
		if e == nil {
			accepted++
		} else if errors.Is(e, ErrInsufficient) {
			insufficient++
		} else {
			t.Fatal(e)
		}
	}
	out := ask(t, r, ref, "state", "get.v1", 0)
	if accepted != 10 || insufficient != 90 || out.State.Reserved != 10 || out.State.Version != 11 {
		t.Fatal(accepted, insufficient, out)
	}
}
func TestAskTimeoutDoesNotWithdrawAcceptedWork(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	o := DefaultOptions()
	var once sync.Once
	o.Hooks.Before = func(ctx context.Context, m Message) error {
		if m.ID == "slow" {
			once.Do(func() {
				close(entered)
				select {
				case <-ctx.Done():
				case <-release:
				}
			})
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return nil
	}
	r := runtimeFixture(t, o)
	ref, e := r.Resolve(testKey)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	raw := wire(t, "slow", "receive.v1", testKey, 4)
	go func() { _, e := r.Ask(ctx, ref, raw); done <- e }()
	await(t, entered)
	cancel()
	if e = <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	close(release)
	eventually(t, func() bool { return r.Stats().AbandonedReplies == 1 })
	out := ask(t, r, ref, "state", "get.v1", 0)
	if out.State.OnHand != 4 {
		t.Fatal("caller canceled accepted effect", out)
	}
	replay, e := r.Ask(context.Background(), ref, raw)
	if e != nil || !replay.Replayed || replay.State.OnHand != 4 {
		t.Fatal(replay, e)
	}
}
func TestSupervisionDropsQueueFencesOldReferencesAndStopsAtBudget(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	o := DefaultOptions()
	o.Mailbox = 2
	o.MaxRestarts = 1
	o.Hooks.Before = func(ctx context.Context, m Message) error {
		if m.ID == "crash-first" {
			close(entered)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
			panic("controlled actor failure")
		}
		if m.ID == "crash-second" {
			panic("controlled repeated failure")
		}
		return nil
	}
	r := runtimeFixture(t, o)
	ref, e := r.Resolve(testKey)
	if e != nil {
		t.Fatal(e)
	}
	ask(t, r, ref, "seed", "receive.v1", 5)
	if _, e = r.Tell(context.Background(), ref, wire(t, "crash-first", "receive.v1", testKey, 1)); e != nil {
		t.Fatal(e)
	}
	await(t, entered)
	if _, e = r.Tell(context.Background(), ref, wire(t, "lost-queued", "receive.v1", testKey, 2)); e != nil {
		t.Fatal(e)
	}
	close(release)
	var fresh Ref
	eventually(t, func() bool { fresh, e = r.Resolve(testKey); return e == nil && fresh.Generation == 2 })
	if _, e = r.Tell(context.Background(), ref, wire(t, "stale", "receive.v1", testKey, 1)); !errors.Is(e, ErrStale) {
		t.Fatal("old generation accepted", e)
	}
	if out := ask(t, r, fresh, "state", "get.v1", 0); out.State.OnHand != 0 {
		t.Fatal("restart pretended to preserve memory", out)
	}
	if len(r.DeadLetters()) != 2 || r.Stats().Dropped != 2 || r.Stats().Restarts != 1 {
		t.Fatal(r.DeadLetters(), r.Stats())
	}
	if _, e = r.Ask(context.Background(), fresh, wire(t, "crash-second", "receive.v1", testKey, 1)); !errors.Is(e, ErrCrashed) {
		t.Fatal(e)
	}
	eventually(t, func() bool { _, e := r.Resolve(testKey); return errors.Is(e, ErrStopped) })
}
func TestReceiptHorizonAndIncarnationAreExplicit(t *testing.T) {
	o := DefaultOptions()
	o.RetainedReceipts = 1
	r := runtimeFixture(t, o)
	ref, e := r.Resolve(testKey)
	if e != nil {
		t.Fatal(e)
	}
	raw := wire(t, "first", "receive.v1", testKey, 1)
	first, e := r.Ask(context.Background(), ref, raw)
	if e != nil {
		t.Fatal(e)
	}
	again, e := r.Ask(context.Background(), ref, raw)
	if e != nil || !again.Replayed || again.State != first.State {
		t.Fatal(again, e)
	}
	changed := wire(t, "first", "receive.v1", testKey, 2)
	if _, e = r.Ask(context.Background(), ref, changed); !errors.Is(e, ErrIdentityConflict) {
		t.Fatal(e)
	}
	ask(t, r, ref, "evict", "receive.v1", 1)
	after, e := r.Ask(context.Background(), ref, raw)
	if e != nil || after.Replayed || after.State.OnHand != 3 {
		t.Fatal("bounded cache claimed permanent dedup", after, e)
	}
	another := runtimeFixture(t, o)
	if _, e = another.Resolve(testKey); e != nil {
		t.Fatal(e)
	}
	if _, e = another.Tell(context.Background(), ref, raw); !errors.Is(e, ErrStale) {
		t.Fatal("reference survived process incarnation", e)
	}
}
func TestWireVersionsBoundsAndLosslessRelay(t *testing.T) {
	raw := wire(t, "version", "receive.v1", testKey, 1)
	old, e := Decode(raw, 1)
	if e != nil || old.Note != "" {
		t.Fatal(old, e)
	}
	newer, e := Decode(raw, 2)
	if e != nil || newer.Note != "annotation" {
		t.Fatal(newer, e)
	}
	preserved, e := PreserveRelay(raw)
	if e != nil || !bytes.Equal(raw, preserved) {
		t.Fatal(e)
	}
	typed, e := TypedOldRelay(raw)
	if e != nil {
		t.Fatal(e)
	}
	lost, e := Decode(typed, 2)
	if e != nil || lost.Note != "" || lost.Hash == newer.Hash {
		t.Fatal("typed old relay did not expose loss", lost, e)
	}
	for _, bad := range [][]byte{bytes.Repeat([]byte("x"), MaxMessageBytes+1), []byte(`{"format":1,"format":1}`), []byte(`[[[[[[[[0]]]]]]]]`), append(append([]byte(nil), raw...), []byte(` {}`)...), bytes.Replace(raw, []byte("receive.v1"), []byte("receive.v9"), 1), bytes.Replace(raw, []byte(`"quantity":1`), []byte(`"quantity":1e100`), 1)} {
		if _, e = Decode(bad, 2); e == nil {
			t.Fatalf("accepted malformed wire: %.80q", bad)
		}
	}
}
func FuzzMessagePreflight(f *testing.F) {
	raw, _ := Encode(2, "seed", testKey, "receive.v1", 1, "note")
	f.Add(raw)
	f.Add([]byte(`{"format":99}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		m, e := Decode(b, 2)
		if e == nil {
			again, e := Decode(m.Raw, 2)
			if e != nil || again.Hash != m.Hash {
				t.Fatal("accepted bytes lost identity")
			}
		}
	})
}

func TestStockTransitionDefendsAgainstUnvalidatedQuantities(t *testing.T) {
	initial := State{Key: testKey, OnHand: 100, Version: 1}
	for _, quantity := range []int64{-1, 0, 1_000_000_001} {
		for _, kind := range []string{"receive.v1", "reserve.v1"} {
			next, e := Evolve(initial, Message{Key: testKey, Kind: kind, Quantity: quantity})
			if !errors.Is(e, ErrInvalid) || next != initial {
				t.Fatal("transport bypass changed stock", next, e)
			}
		}
	}
}

func TestCrashReplyFollowsReferenceFencing(t *testing.T) {
	o := DefaultOptions()
	o.Hooks.Before = func(_ context.Context, m Message) error {
		if m.ID == "crash" {
			panic("controlled processor failure")
		}
		return nil
	}
	r := runtimeFixture(t, o)
	old, e := r.Resolve(testKey)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = r.Ask(context.Background(), old, wire(t, "crash", "receive.v1", testKey, 1)); !errors.Is(e, ErrCrashed) {
		t.Fatal(e)
	}
	fresh, e := r.Resolve(testKey)
	if e != nil || fresh.Generation != old.Generation+1 {
		t.Fatal("failed Ask returned before generation fence", fresh, e)
	}
	if _, e = r.Tell(context.Background(), old, wire(t, "old", "receive.v1", testKey, 1)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
}
