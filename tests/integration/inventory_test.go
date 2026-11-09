//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/changereservation"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/receivestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/reservestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool
var store *postgres.Store

func TestMain(m *testing.M) {
	if err := config.LoadLocalEnv("../../.env"); err != nil {
		fmt.Fprintln(os.Stderr, "could not load test configuration")
		os.Exit(1)
	}
	dsn := os.Getenv("ATLAS_TEST_DATABASE_URL")
	u, err := url.Parse(dsn)
	if err != nil || dsn == "" || u.Path != "/atlas_test" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "postgres") {
		fmt.Fprintln(os.Stderr, "ATLAS_TEST_DATABASE_URL must identify the local atlas_test database; integration tests never silently skip")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid test database configuration")
		os.Exit(1)
	}
	poolConfig.MaxConns = 24
	pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not initialize test database")
		os.Exit(1)
	}
	if err = migrations.Apply(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "test migration failed:", err)
		os.Exit(1)
	}
	store = &postgres.Store{Pool: pool}
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func tenant(t *testing.T) string {
	t.Helper()
	id, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	id = "test-" + id
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"projection_receipts", "stock_views", "outbox", "reservations", "command_results", "stock"} {
			if _, err := pool.Exec(ctx, "DELETE FROM inventory."+table+" WHERE tenant_id=$1", id); err != nil {
				t.Errorf("cleanup %s: %v", table, err)
			}
		}
	})
	return id
}
func receive(t *testing.T, tenant, sku, key string, q int64) ports.Outcome {
	t.Helper()
	out, _, err := (receivestock.Handler{Store: store}).Handle(context.Background(), tenant, key, receivestock.Command{SKU: sku, Quantity: q})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func count(t *testing.T, table, tenant string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM inventory."+table+" WHERE tenant_id=$1", tenant).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func drain(t *testing.T) {
	t.Helper()
	for range 2000 {
		did, err := store.ProcessOne(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !did {
			return
		}
	}
	t.Fatal("outbox did not drain")
}

func TestConcurrentReservationsCannotOversell(t *testing.T) {
	id := tenant(t)
	receive(t, id, "BOOK", "receipt", 10)
	var accepted, rejected atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	start := make(chan struct{})
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := (reservestock.Handler{Store: store}).Handle(context.Background(), id, fmt.Sprintf("reserve-%d", i), reservestock.Command{SKU: "BOOK", Quantity: 1})
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, domain.ErrInsufficientStock):
				rejected.Add(1)
			default:
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if accepted.Load() != 10 || rejected.Load() != 90 {
		t.Fatalf("accepted=%d rejected=%d", accepted.Load(), rejected.Load())
	}
	s, err := store.Stock(context.Background(), id, "BOOK")
	if err != nil {
		t.Fatal(err)
	}
	if s.Reserved != 10 || s.Available() != 0 || s.Version != 11 {
		t.Fatalf("invalid stock: %+v", s)
	}
	if count(t, "reservations", id) != 10 || count(t, "outbox", id) != 11 || count(t, "command_results", id) != 11 {
		t.Fatal("failed commands left durable effects")
	}
}

func TestConcurrentIdempotencyAndPayloadConflict(t *testing.T) {
	id := tenant(t)
	receive(t, id, "BOOK", "receipt", 10)
	results := make(chan ports.Outcome, 40)
	errs := make(chan error, 40)
	var fresh atomic.Int64
	var wg sync.WaitGroup
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, replayed, err := (reservestock.Handler{Store: store}).Handle(context.Background(), id, "same-key", reservestock.Command{SKU: "BOOK", Quantity: 3})
			if err != nil {
				errs <- err
				return
			}
			if !replayed {
				fresh.Add(1)
			}
			results <- out
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	reservationID := ""
	for out := range results {
		if reservationID == "" {
			reservationID = out.Reservation.ID
		}
		if out.Reservation.ID != reservationID || out.Stock.Reserved != 3 || out.Stock.Version != 2 {
			t.Fatal("replays returned different durable outcomes")
		}
	}
	if fresh.Load() != 1 || count(t, "reservations", id) != 1 || count(t, "outbox", id) != 2 {
		t.Fatal("duplicate effect")
	}
	_, _, err := (reservestock.Handler{Store: store}).Handle(context.Background(), id, "same-key", reservestock.Command{SKU: "BOOK", Quantity: 4})
	if !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("payload mismatch accepted: %v", err)
	}
}

func TestTransactionFailureRollsBackStateKeyAndEvent(t *testing.T) {
	id := tenant(t)
	cmd := receivestock.Command{SKU: "BOOK", Quantity: 6}
	req, err := ports.NewRequest(id, "receive-stock", "retry-me", cmd)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected failure after outbox append")
	_, _, err = store.Execute(context.Background(), req, func(ctx context.Context, tx ports.Transaction) (ports.Outcome, error) {
		if err := tx.EnsureStock(ctx, "BOOK"); err != nil {
			return ports.Outcome{}, err
		}
		s, err := tx.LockStock(ctx, "BOOK")
		if err != nil {
			return ports.Outcome{}, err
		}
		if err = s.Receive(6); err != nil {
			return ports.Outcome{}, err
		}
		if err = tx.SaveStock(ctx, s); err != nil {
			return ports.Outcome{}, err
		}
		out := ports.Outcome{Stock: s.Snapshot()}
		if err = tx.AppendEvent(ctx, "inventory.stock-received.v1", out); err != nil {
			return ports.Outcome{}, err
		}
		return out, injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("unexpected failure: %v", err)
	}
	for _, table := range []string{"stock", "outbox", "command_results"} {
		if count(t, table, id) != 0 {
			t.Fatalf("rollback left rows in %s", table)
		}
	}
	out, _, err := (receivestock.Handler{Store: store}).Handle(context.Background(), id, "retry-me", cmd)
	if err != nil || out.Stock.OnHand != 6 {
		t.Fatalf("key was not retryable after rollback: %v", err)
	}
}

func TestCommitReleaseRaceAndTenantIsolation(t *testing.T) {
	id := tenant(t)
	other := tenant(t)
	receive(t, id, "BOOK", "receipt", 10)
	receive(t, other, "BOOK", "receipt", 20)
	out, _, err := (reservestock.Handler{Store: store}).Handle(context.Background(), id, "reserve", reservestock.Command{SKU: "BOOK", Quantity: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Reservation(context.Background(), other, out.Reservation.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("cross-tenant read")
	}
	if _, _, err = (changereservation.Handler{Store: store}).Handle(context.Background(), other, "steal", changereservation.Command{ID: out.Reservation.ID, Target: domain.ReservationCommitted}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("cross-tenant write")
	}
	results := make(chan error, 2)
	for _, target := range []string{domain.ReservationCommitted, domain.ReservationReleased} {
		go func() {
			_, _, err := (changereservation.Handler{Store: store}).Handle(context.Background(), id, target, changereservation.Command{ID: out.Reservation.ID, Target: target})
			results <- err
		}()
	}
	wins, conflicts := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			wins++
		} else if errors.Is(err, domain.ErrInvalidTransition) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatal("both competing terminal transitions succeeded")
	}
	r, err := store.Reservation(context.Background(), id, out.Reservation.ID)
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Stock(context.Background(), id, "BOOK")
	if err != nil {
		t.Fatal(err)
	}
	if s.Reserved != 0 || (r.Status == domain.ReservationCommitted && s.Allocated != 4) || (r.Status == domain.ReservationReleased && s.Allocated != 0) {
		t.Fatalf("inconsistent terminal state: %+v %+v", r, s)
	}
	_, _, err = (changereservation.Handler{Store: store}).Handle(context.Background(), id, "duplicate-terminal", changereservation.Command{ID: r.ID, Target: r.Status})
	if err != nil {
		t.Fatal(err)
	}
	if count(t, "outbox", id) != 3 {
		t.Fatal("repeated terminal action emitted another event")
	}
}

func TestWorkersCannotOvertakeLockedStreamHead(t *testing.T) {
	id := tenant(t)
	receive(t, id, "A", "a1", 1)
	want := receive(t, id, "A", "a2", 1)
	receive(t, id, "B", "b1", 1)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var event string
	if err = tx.QueryRow(context.Background(), `SELECT event_id::text FROM inventory.outbox WHERE tenant_id=$1 AND sku='A' AND aggregate_version=1 FOR UPDATE`, id).Scan(&event); err != nil {
		t.Fatal(err)
	}
	did, err := store.ProcessOne(context.Background())
	if err != nil || !did {
		t.Fatalf("independent stream did not progress: %v", err)
	}
	if _, err = store.Projection(context.Background(), id, "B"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Projection(context.Background(), id, "A"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("second stream event overtook locked predecessor")
	}
	if did, err = store.ProcessOne(context.Background()); err != nil || did {
		t.Fatal("worker consumed a blocked stream")
	}
	if err = tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	drain(t)
	got, err := store.Projection(context.Background(), id, "A")
	if err != nil || got != want.Stock {
		t.Fatalf("projection mismatch: %+v %v", got, err)
	}
}

func TestTwoWorkersAndDuplicateDelivery(t *testing.T) {
	id := tenant(t)
	var want ports.Outcome
	for i := range 30 {
		want = receive(t, id, "BOOK", fmt.Sprintf("receipt-%d", i), 1)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				did, err := store.ProcessOne(context.Background())
				if err != nil {
					errs <- err
					return
				}
				if !did {
					return
				}
			}
			errs <- errors.New("worker failed to drain")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	got, err := store.Projection(context.Background(), id, "BOOK")
	if err != nil || got != want.Stock {
		t.Fatalf("projection mismatch: %+v %v", got, err)
	}
	if count(t, "projection_receipts", id) != 30 {
		t.Fatal("missing projection receipts")
	}
	// Simulate transport redelivery by making an already-processed event eligible.
	if _, err = pool.Exec(context.Background(), `UPDATE inventory.outbox SET processed_at=NULL WHERE tenant_id=$1 AND aggregate_version=1`, id); err != nil {
		t.Fatal(err)
	}
	drain(t)
	after, err := store.Projection(context.Background(), id, "BOOK")
	if err != nil || after != got || count(t, "projection_receipts", id) != 30 {
		t.Fatal("duplicate event changed projection")
	}
}

func TestPoisonEventBlocksOnlyItsStreamAndCanBeRepaired(t *testing.T) {
	id := tenant(t)
	first := receive(t, id, "POISON", "receipt1", 1)
	var original []byte
	if err := pool.QueryRow(context.Background(), `SELECT payload FROM inventory.outbox WHERE tenant_id=$1`, id).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE inventory.outbox SET payload=jsonb_set(payload,'{stock,version}','999') WHERE tenant_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := store.ProcessOne(context.Background()); err == nil {
			t.Fatal("corrupt event was applied")
		}
		if _, err := pool.Exec(context.Background(), `UPDATE inventory.outbox SET available_at=now() WHERE tenant_id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	var quarantined bool
	if err := pool.QueryRow(context.Background(), `SELECT quarantined FROM inventory.outbox WHERE tenant_id=$1`, id).Scan(&quarantined); err != nil || !quarantined {
		t.Fatal("poison event not quarantined")
	}
	want := receive(t, id, "POISON", "receipt2", 1)
	receive(t, id, "HEALTHY", "other", 1)
	drain(t)
	if _, err := store.Projection(context.Background(), id, "POISON"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("event gap was skipped")
	}
	if _, err := store.Projection(context.Background(), id, "HEALTHY"); err != nil {
		t.Fatal("poison event blocked other stream")
	}
	if _, err := pool.Exec(context.Background(), `UPDATE inventory.outbox SET payload=$2,quarantined=false,attempts=0,available_at=now() WHERE tenant_id=$1 AND sku=$3 AND aggregate_version=1`, id, original, first.Stock.SKU); err != nil {
		t.Fatal(err)
	}
	drain(t)
	got, err := store.Projection(context.Background(), id, "POISON")
	if err != nil || got != want.Stock {
		t.Fatal("repaired stream did not resume")
	}
}

func TestMigrationsRejectChangedHistory(t *testing.T) {
	if err := migrations.Apply(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	var name, checksum string
	if err := pool.QueryRow(context.Background(), `SELECT name,checksum FROM public.atlas_schema_migrations ORDER BY name LIMIT 1`).Scan(&name, &checksum); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `UPDATE public.atlas_schema_migrations SET checksum=$2 WHERE name=$1`, name, checksum); err != nil {
			t.Error(err)
		}
	})
	if _, err := pool.Exec(context.Background(), `UPDATE public.atlas_schema_migrations SET checksum='changed' WHERE name=$1`, name); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatal("changed migration was accepted")
	}
}
