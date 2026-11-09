//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/changereservation"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/features/reservestock"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
)

func hold(t *testing.T, tenant, sku, key string, q int64) ports.Outcome {
	t.Helper()
	out, _, err := (reservestock.Handler{Store: store}).Handle(context.Background(), tenant, key, reservestock.Command{SKU: sku, Quantity: q})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func due(t *testing.T, tenant, id string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `UPDATE inventory.reservations SET expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2::uuid`, tenant, id); err != nil {
		t.Fatal(err)
	}
}

func TestExpiryUsesTimeAfterTheLockWait(t *testing.T) {
	tenant := tenant(t)
	receive(t, tenant, "CLOCK", "receipt", 8)
	out := hold(t, tenant, "CLOCK", "hold", 4)
	lock, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(context.Background(), `SELECT 1 FROM inventory.reservations WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, tenant, out.Reservation.ID); err != nil {
		t.Fatal(err)
	}
	command := changereservation.Command{ID: out.Reservation.ID, Target: domain.ReservationCommitted}
	request, err := ports.NewRequest(tenant, "change-reservation", "commit-after-wait", command)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, _, err := store.Execute(context.Background(), request, func(ctx context.Context, tx ports.Transaction) (ports.Outcome, error) {
			close(started)
			return changereservation.Apply(ctx, tx, command)
		})
		done <- err
	}()
	<-started
	// The command's transaction has begun. Set the deadline after that start,
	// then release the row: transaction-start NOW() would incorrectly permit it.
	if _, err = lock.Exec(context.Background(), `UPDATE inventory.reservations SET expires_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2::uuid`, tenant, out.Reservation.ID); err != nil {
		t.Fatal(err)
	}
	if err = lock.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, domain.ErrReservationExpired) {
		t.Fatalf("late commit accepted: %v", err)
	}
	if did, err := store.ExpireOne(context.Background()); err != nil || !did {
		t.Fatalf("expiry failed: %v", err)
	}
	s, err := store.Stock(context.Background(), tenant, "CLOCK")
	if err != nil || s.Reserved != 0 || s.Allocated != 0 || s.Available() != 8 {
		t.Fatalf("expired stock leaked: %+v %v", s, err)
	}
	if did, err := store.ExpireOne(context.Background()); err != nil || did {
		t.Fatal("expiry repeated a durable effect")
	}
	if count(t, "outbox", tenant) != 3 {
		t.Fatal("expected receipt, reservation and expiry events")
	}
}

func TestExpirySkipsLockedReservationsAndReleaseRace(t *testing.T) {
	id := tenant(t)
	receive(t, id, "CLOCK", "receipt", 4)
	a := hold(t, id, "CLOCK", "a", 1)
	b := hold(t, id, "CLOCK", "b", 1)
	due(t, id, a.Reservation.ID)
	due(t, id, b.Reservation.ID)
	lock, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(context.Background(), `SELECT 1 FROM inventory.reservations WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, id, a.Reservation.ID); err != nil {
		t.Fatal(err)
	}
	if did, err := store.ExpireOne(context.Background()); err != nil || !did {
		t.Fatalf("independent reservation blocked: %v", err)
	}
	if did, err := store.ExpireOne(context.Background()); err != nil || did {
		t.Fatal("worker did not skip locked row")
	}
	if err = lock.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	go func() { _, err := store.ExpireOne(context.Background()); errs <- err }()
	go func() {
		_, _, err := (changereservation.Handler{Store: store}).Handle(context.Background(), id, "release", changereservation.Command{ID: a.Reservation.ID, Target: domain.ReservationReleased})
		if errors.Is(err, domain.ErrInvalidTransition) {
			err = nil
		}
		errs <- err
	}()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	s, err := store.Stock(context.Background(), id, "CLOCK")
	if err != nil || s.Available() != 4 || s.Reserved != 0 || s.Version != 5 {
		t.Fatalf("double release or leaked hold: %+v %v", s, err)
	}
	drain(t)
	view, err := store.Projection(context.Background(), id, "CLOCK")
	if err != nil || view != s {
		t.Fatal("expiry projection differs from authority")
	}
}

func TestAllocationReversalIsExplicitAndAppliedOnce(t *testing.T) {
	id := tenant(t)
	receive(t, id, "BOOK", "receipt", 10)
	out := hold(t, id, "BOOK", "hold", 3)
	h := changereservation.Handler{Store: store}
	if _, _, err := h.Handle(context.Background(), id, "early-reversal", changereservation.Command{ID: out.Reservation.ID, Target: domain.ReservationReversed}); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatal("active reservation was reversed")
	}
	if _, _, err := h.Handle(context.Background(), id, "commit", changereservation.Command{ID: out.Reservation.ID, Target: domain.ReservationCommitted}); err != nil {
		t.Fatal(err)
	}
	due(t, id, out.Reservation.ID)
	if did, err := store.ExpireOne(context.Background()); err != nil || did {
		t.Fatal("committed allocation expired")
	}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := h.Handle(context.Background(), id, "reverse", changereservation.Command{ID: out.Reservation.ID, Target: domain.ReservationReversed})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	s, err := store.Stock(context.Background(), id, "BOOK")
	if err != nil || s.Available() != 10 || s.Allocated != 0 || s.Reserved != 0 || s.Version != 4 {
		t.Fatalf("incorrect compensation: %+v %v", s, err)
	}
	if count(t, "outbox", id) != 4 {
		t.Fatal("compensation emitted duplicate events")
	}
}

func TestLegacyReservationResponseSurvivesOptionalTTLUpgrade(t *testing.T) {
	id := tenant(t)
	receive(t, id, "LEGACY", "receipt", 8)
	original := hold(t, id, "LEGACY", "legacy-key", 2)
	// Reproduce the old command shape and saved response, before TTL/deadline
	// fields existed. Never rewrite actual event history to create this fixture.
	legacyBody, _ := json.Marshal(struct {
		SKU      string `json:"sku"`
		Quantity int64  `json:"quantity"`
	}{SKU: "LEGACY", Quantity: 2})
	hash := sha256.Sum256(legacyBody)
	if _, err := pool.Exec(context.Background(), `UPDATE inventory.command_results SET request_hash=$2,response=response #- '{reservation,expires_at}' WHERE tenant_id=$1 AND operation='reserve-stock' AND idempotency_key='legacy-key'`, id, hash[:]); err != nil {
		t.Fatal(err)
	}
	replayed, wasReplay, err := (reservestock.Handler{Store: store}).Handle(context.Background(), id, "legacy-key", reservestock.Command{SKU: "LEGACY", Quantity: 2})
	if err != nil || !wasReplay || replayed.Reservation.ID != original.Reservation.ID || replayed.Stock != original.Stock {
		t.Fatalf("legacy retry changed: %v", err)
	}
	encoded, err := json.Marshal(replayed)
	if err != nil || strings.Contains(string(encoded), "expires_at") {
		t.Fatal("replay invented a deadline absent in the original response")
	}
	if count(t, "reservations", id) != 1 || count(t, "outbox", id) != 2 {
		t.Fatal("legacy retry created another effect")
	}
}
