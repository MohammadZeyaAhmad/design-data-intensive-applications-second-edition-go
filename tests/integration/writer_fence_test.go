//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	invdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/receivestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/database"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func enableWriterFence(t *testing.T) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "UPDATE platform.writer_fence SET enabled=true,epoch=7"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "UPDATE platform.writer_fence SET enabled=false,epoch=1"); err != nil {
			t.Error(err)
		}
	})
}
func writerPool(t *testing.T, token string) *pgxpool.Pool {
	t.Helper()
	c := pool.Config()
	if err := database.ConfigureWriterEpoch(c, token); err != nil {
		t.Fatal(err)
	}
	p, err := pgxpool.NewWithConfig(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
func requireSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var e *pgconn.PgError
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("expected SQLSTATE %s, got %v", code, err)
	}
}
func TestWriterFenceRejectsStaleAndMissingTokenWithoutPartialEffects(t *testing.T) {
	id := tenant(t)
	enableWriterFence(t)
	for _, token := range []string{"", "6", "7"} {
		p := writerPool(t, token)
		s := &invdb.Store{Pool: p}
		_, _, err := (receivestock.Handler{Store: s}).Handle(context.Background(), id, "token-"+token, receivestock.Command{SKU: "BOOK", Quantity: 1})
		if token == "7" {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			requireSQLState(t, err, "55000")
		}
	}
	for _, table := range []string{"stock", "command_results", "outbox"} {
		if count(t, table, id) != 1 {
			t.Fatalf("%s has a stale writer effect", table)
		}
	}
}
func TestWriterFenceEpochAdvanceWaitsForAdmittedTransaction(t *testing.T) {
	id := tenant(t)
	enableWriterFence(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	p := writerPool(t, "7")
	writer, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Release()
	tx, err := writer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "INSERT INTO inventory.stock(tenant_id,sku) VALUES($1,'BOOK')", id); err != nil {
		t.Fatal(err)
	}
	control, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Release()
	done := make(chan error, 1)
	go func() { _, e := control.Exec(ctx, "UPDATE platform.writer_fence SET epoch=8"); done <- e }()
	for {
		var blocked bool
		if err = pool.QueryRow(ctx, "SELECT $1::int = ANY(pg_blocking_pids($2::int))", int(writer.Conn().PgConn().PID()), int(control.Conn().PgConn().PID())).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case e := <-done:
			t.Fatalf("epoch crossed active writer: %v", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	_, err = writer.Exec(ctx, "INSERT INTO inventory.stock(tenant_id,sku) VALUES($1,'STALE')", id)
	requireSQLState(t, err, "55000")
}
func TestWriterFenceRestrictedRoleCanWriteButCannotAdvanceAuthority(t *testing.T) {
	id := tenant(t)
	enableWriterFence(t)
	nonce, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	role := "fence_" + strings.ReplaceAll(nonce, "-", "")
	ident := pgx.Identifier{role}.Sanitize()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err = pool.Exec(ctx, "CREATE ROLE "+ident); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, e := pool.Exec(cleanup, "DROP OWNED BY "+ident+"; DROP ROLE "+ident); e != nil {
			t.Error(e)
		}
	})
	if _, err = pool.Exec(ctx, "GRANT USAGE ON SCHEMA inventory,platform TO "+ident+"; GRANT INSERT,SELECT ON inventory.stock TO "+ident); err != nil {
		t.Fatal(err)
	}
	c, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	defer c.Exec(context.Background(), "RESET ROLE; RESET atlas.writer_epoch")
	if _, err = c.Exec(ctx, "SET ROLE "+ident+"; SET atlas.writer_epoch='7'"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, "INSERT INTO inventory.stock(tenant_id,sku) VALUES($1,'BOOK')", id); err != nil {
		t.Fatal(err)
	}
	_, err = c.Exec(ctx, "UPDATE platform.writer_fence SET epoch=8")
	requireSQLState(t, err, "42501")
	_, err = c.Exec(ctx, "ALTER TABLE inventory.stock DISABLE TRIGGER writer_fence")
	requireSQLState(t, err, "42501")
}
