//go:build integration

package durable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/disposabledatabase"
)

func fixture(t *testing.T) Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, e := disposabledatabase.Create(ctx, os.Getenv("ATLAS_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e = db.Close(); e != nil {
			t.Error(e)
		}
	})
	if _, e = db.Pool.Exec(ctx, Schema); e != nil {
		t.Fatal(e)
	}
	return Store{Pool: db.Pool}
}

var key = actors.Key{Tenant: "acme", SKU: "BOOK"}

func message(t *testing.T, id, kind string, q int64) actors.Message {
	t.Helper()
	raw, e := actors.Encode(2, id, key, kind, q, "keep unknown annotation")
	if e != nil {
		t.Fatal(e)
	}
	m, e := actors.Decode(raw, 1)
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func TestDurableReceiptStateAndRawSourceAreAtomic(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	m := message(t, "same-id", "receive.v1", 5)
	first, e := s.Apply(ctx, "group-a", "worker-a", m, Source{})
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.Apply(ctx, "group-a", "worker-b", m, Source{})
	if e != nil || !again.Replayed || again.State != first.State {
		t.Fatal(again, e)
	}
	if _, e = s.Apply(ctx, "group-a", "worker-b", message(t, "same-id", "receive.v1", 6), Source{}); !errors.Is(e, actors.ErrIdentityConflict) {
		t.Fatal(e)
	}
	other, e := s.Apply(ctx, "group-b", "subscriber", m, Source{})
	if e != nil || other.Replayed || other.State.OnHand != 5 {
		t.Fatal(other, e)
	}
	var raw []byte
	var receipts, attempts int
	if e = s.Pool.QueryRow(ctx, `SELECT source_body FROM actors.receipts WHERE subscription='group-a'`).Scan(&raw); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(raw, m.Raw) {
		t.Fatal("old reader rewrote unknown source fields")
	}
	if e = s.Pool.QueryRow(ctx, `SELECT count(*) FROM actors.receipts WHERE subscription='group-a'`).Scan(&receipts); e != nil {
		t.Fatal(e)
	}
	if e = s.Pool.QueryRow(ctx, `SELECT count(*) FROM actors.attempts WHERE subscription='group-a'`).Scan(&attempts); e != nil {
		t.Fatal(e)
	}
	if receipts != 1 || attempts != 2 {
		t.Fatal(receipts, attempts)
	}
}
func TestCanceledBeforeCommitRollsBackStateAndReceipt(t *testing.T) {
	s := fixture(t)
	entered := make(chan struct{})
	s.BeforeCommit = func(ctx context.Context, _ actors.Message) error { close(entered); <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	m := message(t, "interrupted", "receive.v1", 5)
	go func() { _, e := s.Apply(ctx, "rollback", "worker", m, Source{}); done <- e }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("commit checkpoint not reached")
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	var n int
	if e := s.Pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM actors.stock)+(SELECT count(*) FROM actors.receipts)+(SELECT count(*) FROM actors.attempts)`).Scan(&n); e != nil || n != 0 {
		t.Fatal("partial commit", n, e)
	}
	s.BeforeCommit = nil
	out, e := s.Apply(context.Background(), "rollback", "replacement", m, Source{})
	if e != nil || out.Replayed || out.State.OnHand != 5 {
		t.Fatal(out, e)
	}
}
func TestTwoIndependentActorOwnersStillNeedDatabaseConcurrencyControl(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	if _, e := s.Apply(ctx, "shared", "seed", message(t, "seed", "receive.v1", 10), Source{}); e != nil {
		t.Fatal(e)
	}
	var nodes []*actors.Runtime
	for i := 0; i < 2; i++ {
		o := actors.DefaultOptions()
		o.Mailbox = 128
		worker := fmt.Sprintf("node-%d", i)
		o.Factory = func(actors.Key) actors.Processor {
			return func(ctx context.Context, m actors.Message) (actors.Reply, error) {
				return s.Apply(ctx, "shared", worker, m, Source{})
			}
		}
		r, e := actors.New(ctx, o)
		if e != nil {
			t.Fatal(e)
		}
		nodes = append(nodes, r)
		t.Cleanup(func() {
			stop, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = r.Close(stop)
		})
	}
	refs := make([]actors.Ref, 2)
	for i, r := range nodes {
		var e error
		refs[i], e = r.Resolve(key)
		if e != nil {
			t.Fatal(e)
		}
	}
	errs := make(chan error, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		m := message(t, fmt.Sprintf("take-%d", i), "reserve.v1", 1)
		node := i % 2
		wg.Add(1)
		go func() { defer wg.Done(); _, e := nodes[node].Ask(ctx, refs[node], m.Raw); errs <- e }()
	}
	wg.Wait()
	close(errs)
	accepted := 0
	for e := range errs {
		if e == nil {
			accepted++
		} else if !errors.Is(e, actors.ErrInsufficient) {
			t.Fatal(e)
		}
	}
	state, e := s.State(ctx, "shared", key)
	if e != nil || accepted != 10 || state.Reserved != 10 || state.Version != 11 {
		t.Fatal(state, accepted, e)
	}
}
