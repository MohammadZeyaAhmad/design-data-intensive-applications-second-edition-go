//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/readrouting"
	"github.com/jackc/pgx/v5"
)

func sessionRouter(t *testing.T) *readrouting.Router {
	t.Helper()
	r, e := readrouting.New(context.Background(), readrouting.Options{Primary: pool, Key: []byte(strings.Repeat("s", 32)), Shard: "inventory"})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestSessionWatermarkAndExplicitReplicaTimeout(t *testing.T) {
	id := tenant(t)
	receive(t, id, "BOOK", "seed", 10)
	r := sessionRouter(t)
	m, e := r.AfterWrite(context.Background(), id, "")
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Verify(context.Background(), "other", m.Token); !errors.Is(e, readrouting.ErrToken) {
		t.Fatal(e)
	}
	called := false
	_, e = r.Read(context.Background(), id, readrouting.ReadOptions{Token: m.Token, Mode: "replica", OnLag: "pending"}, func(context.Context, pgx.Tx) error { called = true; return nil })
	if !errors.Is(e, readrouting.ErrPending) || called {
		t.Fatal("pending exposed a snapshot", e)
	}
	next, e := r.Read(context.Background(), id, readrouting.ReadOptions{Token: m.Token, Mode: "replica", OnLag: "primary"}, func(ctx context.Context, tx pgx.Tx) error {
		var onHand int64
		err := tx.QueryRow(ctx, "SELECT on_hand FROM inventory.stock WHERE tenant_id=$1 AND sku='BOOK'", id).Scan(&onHand)
		if err == nil && onHand != 10 {
			t.Error("lost acknowledged write")
		}
		return err
	})
	if e != nil || next.Source != "primary" || next.Reason != "replica-timeout-primary" || next.Token == "" {
		t.Fatal(next, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e = r.Read(ctx, id, readrouting.ReadOptions{Mode: "replica", OnLag: "pending", Wait: time.Second}, func(context.Context, pgx.Tx) error { return nil })
	if !errors.Is(e, context.Canceled) {
		t.Fatal("wait ignored cancellation", e)
	}
}
func TestSessionSnapshotExcludesInterleavedCommittedWrite(t *testing.T) {
	id := tenant(t)
	receive(t, id, "BOOK", "seed", 10)
	r := sessionRouter(t)
	_, e := r.Read(context.Background(), id, readrouting.ReadOptions{Mode: "primary", OnLag: "pending"}, func(ctx context.Context, tx pgx.Tx) error {
		var before, after int64
		if err := tx.QueryRow(ctx, "SELECT on_hand FROM inventory.stock WHERE tenant_id=$1 AND sku='BOOK'", id).Scan(&before); err != nil {
			return err
		}
		receive(t, id, "BOOK", "interleaved", 7)
		if err := tx.QueryRow(ctx, "SELECT on_hand FROM inventory.stock WHERE tenant_id=$1 AND sku='BOOK'", id).Scan(&after); err != nil {
			return err
		}
		if before != 10 || after != 10 {
			t.Fatalf("RR snapshot changed during concurrent commit: %d %d", before, after)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	stock, e := store.Stock(context.Background(), id, "BOOK")
	if e != nil || stock.OnHand != 17 {
		t.Fatal(stock, e)
	}
}
func TestSessionHistoryChangeStopsOldAuthority(t *testing.T) {
	r := sessionRouter(t)
	ctx := context.Background()
	m, e := r.AfterWrite(ctx, "acme", "")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, "UPDATE platform.writer_fence SET epoch=2"); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), "UPDATE platform.writer_fence SET epoch=1")
		if err != nil {
			t.Error(err)
		}
	})
	if e = r.Verify(ctx, "acme", m.Token); !errors.Is(e, readrouting.ErrHistory) {
		t.Fatal("old API retained authority", e)
	}
	fresh := sessionRouter(t)
	if e = fresh.Verify(ctx, "acme", m.Token); !errors.Is(e, readrouting.ErrHistory) {
		t.Fatal("old token crossed history", e)
	}
}

func TestSessionReadIdentityIsPublicContractWithPrivateAuthority(t *testing.T) {
	ctx := context.Background()
	nonce, e := identity.NewID()
	if e != nil {
		t.Fatal(e)
	}
	role := "read_" + strings.ReplaceAll(nonce, "-", "")
	ident := pgx.Identifier{role}.Sanitize()
	if _, e = pool.Exec(ctx, "CREATE ROLE "+ident+"; GRANT USAGE ON SCHEMA platform TO "+ident); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := pool.Exec(context.Background(), "DROP OWNED BY "+ident+"; DROP ROLE "+ident); e != nil {
			t.Error(e)
		}
	})
	c, e := pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Release()
	defer c.Exec(context.Background(), "RESET ROLE")
	if _, e = c.Exec(ctx, "SET ROLE "+ident); e != nil {
		t.Fatal(e)
	}
	var source string
	var epoch int64
	if e = c.QueryRow(ctx, "SELECT source_id::text,epoch FROM platform.read_identity()").Scan(&source, &epoch); e != nil || len(source) != 36 || epoch < 1 {
		t.Fatal("restricted identity contract", source, epoch, e)
	}
	_, e = c.Exec(ctx, "UPDATE platform.read_source SET source_id=gen_random_uuid()")
	requireSQLState(t, e, "42501")
	_, e = c.Exec(ctx, "UPDATE platform.writer_fence SET epoch=epoch+1")
	requireSQLState(t, e, "42501")
}

func TestSessionSourceIdentityRejectsUnrelatedAuthority(t *testing.T) {
	ctx := context.Background()
	r := sessionRouter(t)
	m, e := r.AfterWrite(ctx, "acme", "")
	if e != nil {
		t.Fatal(e)
	}
	var original string
	if e = pool.QueryRow(ctx, "SELECT source_id::text FROM platform.read_identity()").Scan(&original); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := pool.Exec(context.Background(), "UPDATE platform.read_source SET source_id=$1::uuid", original); e != nil {
			t.Error(e)
		}
	})
	if _, e = pool.Exec(ctx, "UPDATE platform.read_source SET source_id=gen_random_uuid()"); e != nil {
		t.Fatal(e)
	}
	if e = r.Verify(ctx, "acme", m.Token); !errors.Is(e, readrouting.ErrHistory) {
		t.Fatal("source replacement retained old authority", e)
	}
	if e = sessionRouter(t).Verify(ctx, "acme", m.Token); !errors.Is(e, readrouting.ErrHistory) {
		t.Fatal("unrelated source accepted token", e)
	}
}
