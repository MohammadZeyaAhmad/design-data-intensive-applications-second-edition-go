//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/amendorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/cancelorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/placeorder"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

func orderStore() *orders.Store { return &orders.Store{Pool: pool} }
func place(t *testing.T, tenant, key string) domain.State {
	t.Helper()
	state, _, err := (placeorder.Handler{Store: orderStore()}).Handle(context.Background(), tenant, key, placeorder.Command{Lines: []domain.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func amend(t *testing.T, tenant, key string, state domain.State, q int64) domain.State {
	t.Helper()
	out, _, err := (amendorder.Handler{Store: orderStore()}).Handle(context.Background(), tenant, key, amendorder.Command{ID: state.ID, ExpectedVersion: state.Version, Lines: []domain.Line{{SKU: "BOOK", Quantity: q}}})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func orderCount(t *testing.T, table, tenant string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM ordering."+table+" WHERE tenant_id=$1", tenant).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func drainOrders(t *testing.T) {
	t.Helper()
	for range 3000 {
		did, err := orderStore().ProcessOne(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !did {
			return
		}
	}
	t.Fatal("ordering outbox did not drain")
}

func TestOrderExpectedVersionAndSavedResponse(t *testing.T) {
	id := tenant(t)
	state := place(t, id, "place")
	var wins, conflicts atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := (amendorder.Handler{Store: orderStore()}).Handle(context.Background(), id, fmt.Sprintf("amend-%d", i), amendorder.Command{ID: state.ID, ExpectedVersion: 1, Lines: []domain.Line{{SKU: "BOOK", Quantity: int64(i + 1)}}})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, domain.ErrConflict):
				conflicts.Add(1)
			default:
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if wins.Load() != 1 || conflicts.Load() != 19 {
		t.Fatalf("competing appends: wins=%d conflicts=%d", wins.Load(), conflicts.Load())
	}
	current, err := orderStore().Load(context.Background(), id, state.ID)
	if err != nil || current.Version != 2 {
		t.Fatal("stream lost an update")
	}
	if orderCount(t, "events", id) != 2 || orderCount(t, "outbox", id) != 2 || orderCount(t, "command_results", id) != 2 {
		t.Fatal("failed append left durable rows")
	}
	replayed, wasReplay, err := (placeorder.Handler{Store: orderStore()}).Handle(context.Background(), id, "place", placeorder.Command{Lines: []domain.Line{{SKU: "BOOK", Quantity: 2}}})
	if err != nil || !wasReplay || !reflect.DeepEqual(replayed, state) {
		t.Fatal("retry returned current state instead of original outcome")
	}
}

func TestConcurrentOrderIdempotencyAndTenantIsolation(t *testing.T) {
	id := tenant(t)
	other := tenant(t)
	var wg sync.WaitGroup
	results := make(chan domain.State, 20)
	errs := make(chan error, 20)
	var fresh atomic.Int64
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, replayed, err := (placeorder.Handler{Store: orderStore()}).Handle(context.Background(), id, "same", placeorder.Command{Lines: []domain.Line{{SKU: "BOOK", Quantity: 2}}})
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
	var state domain.State
	for out := range results {
		if state.ID == "" {
			state = out
		}
		if !reflect.DeepEqual(out, state) {
			t.Fatal("duplicate place created another order")
		}
	}
	if fresh.Load() != 1 || orderCount(t, "events", id) != 1 || orderCount(t, "streams", id) != 1 {
		t.Fatal("duplicate event effect")
	}
	_, _, err := (placeorder.Handler{Store: orderStore()}).Handle(context.Background(), id, "same", placeorder.Command{Lines: []domain.Line{{SKU: "BOOK", Quantity: 3}}})
	if !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatal("changed payload reused request identity")
	}
	if _, err = orderStore().Load(context.Background(), other, state.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("cross-tenant replay")
	}
	if _, _, err = (cancelorder.Handler{Store: orderStore()}).Handle(context.Background(), other, "cancel", cancelorder.Command{ID: state.ID, ExpectedVersion: 1, Reason: "customer_requested"}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("cross-tenant cancellation")
	}
	upper, err := orderStore().Load(context.Background(), id, strings.ToUpper(state.ID))
	if err != nil || upper.ID != state.ID {
		t.Fatal("UUID spelling changed aggregate identity")
	}
}

func TestOrderAppendFailureIsAtomic(t *testing.T) {
	id := tenant(t)
	stream, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	req, err := ports.NewRequest(id, "broken-batch", "retry", map[string]string{"fixture": "invalid-second-event"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = orderStore().Execute(context.Background(), req, stream, 0, func(o *domain.Order) ([]domain.Event, error) {
		events, err := o.Place([]domain.Line{{SKU: "BOOK", Quantity: 1}})
		if err != nil {
			return nil, err
		}
		return append(events, domain.Event{Version: 2, Type: "unknown.future", SchemaVersion: 99, Data: json.RawMessage(`{}`)}), nil
	})
	if !errors.Is(err, domain.ErrUnsupportedSchema) {
		t.Fatalf("unexpected append outcome: %v", err)
	}
	for _, table := range []string{"streams", "events", "outbox", "command_results"} {
		if orderCount(t, table, id) != 0 {
			t.Fatalf("partial append in %s", table)
		}
	}
}

func TestSnapshotsReplayAndKeysetHistory(t *testing.T) {
	id := tenant(t)
	state := place(t, id, "place")
	state = amend(t, id, "amend", state, 3)
	snapshot, err := orderStore().Snapshot(context.Background(), id, state.ID)
	if err != nil || !reflect.DeepEqual(snapshot, state) {
		t.Fatal("snapshot differs from authoritative replay")
	}
	state, _, err = (cancelorder.Handler{Store: orderStore()}).Handle(context.Background(), id, "cancel", cancelorder.Command{ID: state.ID, ExpectedVersion: 2, Reason: "customer_requested"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := orderStore().Load(context.Background(), id, state.ID)
	if err != nil || !reflect.DeepEqual(got, state) {
		t.Fatal("snapshot suffix replay failed")
	}
	full, err := orderStore().Replay(context.Background(), id, state.ID)
	if err != nil || !reflect.DeepEqual(full, got) {
		t.Fatal("cached and full replay disagree")
	}
	for _, corruption := range []string{`UPDATE ordering.snapshots SET checksum=decode(repeat('00',32),'hex') WHERE tenant_id=$1`, `UPDATE ordering.snapshots SET schema_version=999 WHERE tenant_id=$1`} {
		if _, err = pool.Exec(context.Background(), corruption, id); err != nil {
			t.Fatal(err)
		}
		got, err = orderStore().Load(context.Background(), id, state.ID)
		if err != nil || !reflect.DeepEqual(got, state) {
			t.Fatal("invalid snapshot did not fall back to history")
		}
	}
	page, err := orderStore().Events(context.Background(), id, state.ID, 0, 2)
	if err != nil || len(page.Events) != 2 || page.NextAfterVersion != 2 {
		t.Fatal("first keyset page invalid")
	}
	page, err = orderStore().Events(context.Background(), id, state.ID, page.NextAfterVersion, 2)
	if err != nil || len(page.Events) != 1 || page.Events[0].Version != 3 {
		t.Fatal("keyset pagination skipped or duplicated an event")
	}
	if _, err = orderStore().Events(context.Background(), id, state.ID, 0, 101); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal("unbounded history page accepted")
	}
}

func TestLegacyOrderEventsRemainImmutableAndProjectable(t *testing.T) {
	id := tenant(t)
	stream, _ := identity.NewID()
	req, _ := ports.NewRequest(id, "legacy-place", "legacy", struct{}{})
	state, _, err := orderStore().Execute(context.Background(), req, stream, 0, func(*domain.Order) ([]domain.Event, error) {
		return []domain.Event{{Version: 1, Type: domain.OrderPlaced, SchemaVersion: 1, Data: json.RawMessage(`{"items":[{"sku":"BOOK","qty":4}]}`)}}, nil
	})
	if err != nil || state.Lines[0].Quantity != 4 {
		t.Fatalf("upcast failed: %v", err)
	}
	for _, sql := range []string{`UPDATE ordering.events SET schema_version=2 WHERE tenant_id=$1`, `DELETE FROM ordering.events WHERE tenant_id=$1`} {
		if _, err = pool.Exec(context.Background(), sql, id); err == nil {
			t.Fatal("historical event mutation was allowed")
		}
	}
	drainOrders(t)
	view, err := orderStore().Projection(context.Background(), id, stream)
	if err != nil || !reflect.DeepEqual(view, state) {
		t.Fatal("legacy projection differs from replay")
	}
	page, err := orderStore().Events(context.Background(), id, stream, 0, 10)
	if err != nil || page.Events[0].SchemaVersion != 1 {
		t.Fatal("upcaster rewrote historical bytes")
	}
}

func TestProjectionRebuildConcurrentWithAppendsAndDuplicateDelivery(t *testing.T) {
	id := tenant(t)
	state := place(t, id, "place")
	drainOrders(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 20 {
			out, _, err := (amendorder.Handler{Store: orderStore()}).Handle(context.Background(), id, fmt.Sprintf("edit-%d", i), amendorder.Command{ID: state.ID, ExpectedVersion: int64(i + 1), Lines: []domain.Line{{SKU: "BOOK", Quantity: int64(i + 1)}}})
			if err != nil {
				errs <- err
				return
			}
			if out.Version != int64(i+2) {
				errs <- errors.New("wrong append version")
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 20 {
			if _, err := orderStore().Rebuild(context.Background(), id, state.ID); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	drainOrders(t)
	full, err := orderStore().Replay(context.Background(), id, state.ID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := orderStore().Projection(context.Background(), id, state.ID)
	if err != nil || !reflect.DeepEqual(view, full) || view.Version != 21 {
		t.Fatal("online rebuild skipped committed events")
	}
	if _, err = pool.Exec(context.Background(), `UPDATE ordering.outbox SET processed_at=NULL WHERE tenant_id=$1 AND version=1`, id); err != nil {
		t.Fatal(err)
	}
	drainOrders(t)
	view, err = orderStore().Projection(context.Background(), id, state.ID)
	if err != nil || !reflect.DeepEqual(view, full) || orderCount(t, "projection_receipts", id) != 21 {
		t.Fatal("redelivery changed rebuilt view")
	}
	if _, err = pool.Exec(context.Background(), `DELETE FROM ordering.order_views WHERE tenant_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = orderStore().Projection(context.Background(), id, state.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("derived view was not removed")
	}
	if rebuilt, err := orderStore().Rebuild(context.Background(), id, state.ID); err != nil || !reflect.DeepEqual(rebuilt, full) {
		t.Fatal("missing view was not reconstructed from events")
	}
	latest := amend(t, id, "after-rebuild", full, 42)
	drainOrders(t)
	view, err = orderStore().Projection(context.Background(), id, state.ID)
	if err != nil || !reflect.DeepEqual(view, latest) {
		t.Fatal("live processing did not resume after rebuild")
	}
}

func TestUnsupportedEventSchemaQuarantinesWithoutSkipping(t *testing.T) {
	drainOrders(t)
	id := tenant(t)
	state := place(t, id, "place")
	drainOrders(t)
	// Model an event emitted by a future binary, never an in-place edit.
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var eventID string
	if err = tx.QueryRow(context.Background(), `INSERT INTO ordering.events (tenant_id,order_id,version,command_id,event_type,schema_version,payload) VALUES ($1,$2::uuid,2,gen_random_uuid(),'order.future',999,'{}') RETURNING event_id::text`, id, state.ID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(context.Background(), `INSERT INTO ordering.outbox (tenant_id,order_id,version,event_id) VALUES ($1,$2::uuid,2,$3::uuid)`, id, state.ID, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(context.Background(), `UPDATE ordering.streams SET version=2 WHERE tenant_id=$1 AND id=$2::uuid`, id, state.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = orderStore().Load(context.Background(), id, state.ID); !errors.Is(err, domain.ErrUnsupportedSchema) {
		t.Fatal("unknown source event was ignored")
	}
	for range 5 {
		if _, err = orderStore().ProcessOne(context.Background()); !errors.Is(err, domain.ErrUnsupportedSchema) {
			t.Fatalf("unexpected consumer result: %v", err)
		}
		if _, err = pool.Exec(context.Background(), `UPDATE ordering.outbox SET available_at=now() WHERE tenant_id=$1 AND version=2`, id); err != nil {
			t.Fatal(err)
		}
	}
	healthy := place(t, tenant(t), "healthy")
	drainOrders(t)
	if _, err = orderStore().Projection(context.Background(), healthy.TenantID, healthy.ID); err != nil {
		t.Fatal("unsupported stream blocked independent order")
	}
	view, err := orderStore().Projection(context.Background(), id, state.ID)
	if err != nil || view.Version != 1 {
		t.Fatal("unsupported event advanced the view")
	}
}
