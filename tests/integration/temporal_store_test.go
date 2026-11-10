//go:build integration

package integration

import (
	"context"
	"errors"
	"reflect"
	"testing"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/operations"
	catdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	checkdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	checktemporal "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/temporal"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/ports"
)

func temporalPending(t *testing.T, method string) (*commerce, ports.TemporalInput) {
	t.Helper()
	h := commerceHarness(t)
	h.orderService.Engine = "temporal"
	tenant := commerceTenant(t)
	product(t, h, tenant, "BOOK", "p", 100, 0)
	receive(t, tenant, "BOOK", "stock", 10)
	v, _ := beginCheckout(t, h, tenant, method, []catdomain.Line{{SKU: "BOOK", Quantity: 2}})
	t.Cleanup(func() {
		_, err := pool.Exec(context.Background(), `UPDATE checkout.jobs SET temporal_started_at=COALESCE(temporal_started_at,now()) WHERE tenant_id=$1 AND engine='temporal'`, tenant)
		if err != nil {
			t.Error(err)
		}
	})
	return h, ports.TemporalInput{Execution: ports.Execution{TenantID: tenant, ID: v.ID}}
}
func runTemporalSteps(t *testing.T, h *commerce, in ports.TemporalInput) domain.State {
	t.Helper()
	a := checktemporal.Activities{Store: h.jobs, Handler: h.worker.Handler}
	for n := 0; n < 30; n++ {
		out, err := a.Advance(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		if domain.Terminal(out.State.Stage) {
			return out.State
		}
		if out.Wait == "" {
			in.Step++
		}
	}
	t.Fatal("Temporal activities did not converge")
	return domain.State{}
}

func TestTemporalOwnerPinnedAndPostgresRefusesJob(t *testing.T) {
	h, in := temporalPending(t, "test-ok")
	ctx := context.Background()
	v, err := h.jobs.Get(ctx, in.TenantID, in.ID)
	if err != nil || v.Engine != "temporal" {
		t.Fatal(v, err)
	}
	if _, found, err := h.jobs.Claim(ctx); err != nil || found {
		t.Fatal("PostgreSQL stole Temporal work", found, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE checkout.jobs SET engine='postgres' WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID); err == nil {
		t.Fatal("owner changed")
	}
	if _, err = pool.Exec(ctx, `UPDATE checkout.jobs SET request=request||'{"engine":"postgres"}'::jsonb WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID); err == nil {
		t.Fatal("accepted request changed")
	}
	var quoteID string
	if err = pool.QueryRow(ctx, `SELECT request->'quote'->>'id' FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID).Scan(&quoteID); err != nil {
		t.Fatal(err)
	}
	q, err := h.catalog.Quote(ctx, in.TenantID, quoteID)
	if err != nil {
		t.Fatal(err)
	}
	h.orderService.Engine = "postgres"
	replay, replayed, err := h.orderService.Begin(ctx, in.TenantID, v.OrderID, "checkout", 1, q, "test-ok")
	if err != nil || !replayed || replay.ID != in.ID {
		t.Fatal("profile switch changed acceptance replay", replay, replayed, err)
	}
	pg, tenant, accepted := pendingCheckout(t)
	if _, _, err = pg.jobs.TemporalClaim(ctx, ports.TemporalInput{Execution: ports.Execution{TenantID: tenant, ID: accepted.ID}}); !errors.Is(err, ports.ErrOwner) {
		t.Fatal("Temporal accepted PostgreSQL job", err)
	}
}

func TestTemporalActivityReceiptSurvivesLostCompletion(t *testing.T) {
	h, in := temporalPending(t, "test-lost-response")
	ctx := context.Background()
	a := checktemporal.Activities{Store: h.jobs, Handler: h.worker.Handler}
	first, err := a.Advance(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := a.Advance(ctx, in)
	if err != nil || !reflect.DeepEqual(first, replay) {
		t.Fatal("completion loss advanced twice", first, replay, err)
	}
	var step, receipts int
	if err = pool.QueryRow(ctx, `SELECT temporal_step,(SELECT count(*) FROM checkout.temporal_steps WHERE tenant_id=$1 AND id=$2::uuid) FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID).Scan(&step, &receipts); err != nil || step != 1 || receipts != 1 {
		t.Fatal(step, receipts, err)
	}
	if state := runTemporalSteps(t, h, in); state.Stage != domain.Confirmed {
		t.Fatal(state)
	}
	assertTemporalCaptureOnce(t, in.Execution)
	if _, err = pool.Exec(ctx, `DELETE FROM checkout.temporal_steps WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID); err == nil {
		t.Fatal("receipt was mutable")
	}
}

func assertTemporalCaptureOnce(t *testing.T, e ports.Execution) {
	t.Helper()
	var count int
	for _, query := range []string{
		`SELECT count(*) FROM payments.journal WHERE tenant_id=$1 AND payment_id=$2::uuid AND kind='capture'`,
		`SELECT count(*) FROM provider.operations WHERE tenant_id=$1 AND id=$2::uuid AND status='captured'`,
	} {
		if err := pool.QueryRow(context.Background(), query, e.TenantID, e.ID).Scan(&count); err != nil || count != 1 {
			t.Fatal("duplicate or missing capture", count, err)
		}
	}
}

func TestTemporalStaleActivityCannotCheckpoint(t *testing.T) {
	h, in := temporalPending(t, "test-ok")
	ctx := context.Background()
	old, cached, err := h.jobs.TemporalClaim(ctx, in)
	if err != nil || cached != nil {
		t.Fatal(cached, err)
	}
	next, err := h.worker.Handler.Handle(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE checkout.jobs SET lease_until=now()-interval '1 second' WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID); err != nil {
		t.Fatal(err)
	}
	fresh, cached, err := h.jobs.TemporalClaim(ctx, in)
	if err != nil || cached != nil || fresh.Token <= old.Token {
		t.Fatal("lease not fenced", fresh.Token, err)
	}
	if _, err = h.jobs.TemporalSave(ctx, old, in.Step, next, false); !errors.Is(err, checkdb.ErrLostLease) {
		t.Fatal("stale completion saved", err)
	}
	next, err = h.worker.Handler.Handle(ctx, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.jobs.TemporalSave(ctx, fresh, in.Step, next, false); err != nil {
		t.Fatal(err)
	}
	if state := runTemporalSteps(t, h, in); state.Stage != domain.Confirmed {
		t.Fatal(state)
	}
	var reserved int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM inventory.reservations WHERE tenant_id=$1`, in.TenantID).Scan(&reserved); err != nil || reserved != 1 {
		t.Fatal("recovered activity duplicated hold", reserved, err)
	}
}

func TestTemporalPauseCancellationWakeAndReceiptReplay(t *testing.T) {
	h, in := temporalPending(t, "test-ok")
	ctx := context.Background()
	a := checktemporal.Activities{Store: h.jobs, Handler: h.worker.Handler}
	if _, err := a.Advance(ctx, in); err != nil {
		t.Fatal(err)
	}
	control(t, h.jobs, operatorContext(t, in.TenantID), in.TenantID, in.ID, "pause", "pause", 0)
	// A native cancellation must persist even when the activity response was
	// already committed and the operator has paused subsequent progress.
	in.ForceCancel = true
	if _, err := a.Advance(ctx, in); err != nil {
		t.Fatal(err)
	}
	in.Step++
	out, err := a.Advance(ctx, in)
	if err != nil || out.Wait != "paused" {
		t.Fatal(out, err)
	}
	v, err := h.jobs.Get(ctx, in.TenantID, in.ID)
	if err != nil || !v.CancelRequested || v.CancellationMetadata.Actor != "worker:checkout" {
		t.Fatal(v, err)
	}
	control(t, h.jobs, operatorContext(t, in.TenantID), in.TenantID, in.ID, "resume", "resume", 1)
	if state := runTemporalSteps(t, h, in); state.Stage != domain.Cancelled {
		t.Fatal(state)
	}
	var wakes int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM checkout.temporal_wakes WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID).Scan(&wakes); err != nil || wakes != 3 {
		t.Fatal("controls lost wake outbox", wakes, err)
	}
	if _, _, err = h.jobs.Control(operatorContext(t, in.TenantID), api.Command{TenantID: in.TenantID, ID: in.ID, Action: "retry", Reason: "investigation", Key: "terminal", ExpectedVersion: 2}); !errors.Is(err, api.ErrTerminal) {
		t.Fatal("terminal retry reopened workflow", err)
	}
}

type uncertainTemporalStart struct {
	calls []ports.Execution
	fail  bool
}

func (s *uncertainTemporalStart) Start(_ context.Context, e ports.Execution) error {
	s.calls = append(s.calls, e)
	if s.fail {
		s.fail = false
		return errors.New("lost Temporal start response")
	}
	return nil
}
func (s *uncertainTemporalStart) Wake(_ context.Context, e ports.Execution) error {
	return s.Start(context.Background(), e)
}

func TestTemporalDispatcherRetainsUncertainStartAndWake(t *testing.T) {
	h, in := temporalPending(t, "test-ok")
	ctx := context.Background()
	s := &uncertainTemporalStart{fail: true}
	if found, err := h.jobs.StartTemporalOne(ctx, s); !found || err == nil {
		t.Fatal("uncertain start acknowledged", found, err)
	}
	if found, err := h.jobs.StartTemporalOne(ctx, s); !found || err != nil {
		t.Fatal(found, err)
	}
	if len(s.calls) != 2 || s.calls[0] != in.Execution || s.calls[1] != in.Execution {
		t.Fatal("start retry changed identity", s.calls)
	}
	control(t, h.jobs, operatorContext(t, in.TenantID), in.TenantID, in.ID, "pause", "pause", 0)
	s.fail = true
	if found, err := h.jobs.WakeTemporalOne(ctx, s); !found || err == nil {
		t.Fatal("uncertain signal acknowledged", found, err)
	}
	if found, err := h.jobs.WakeTemporalOne(ctx, s); !found || err != nil {
		t.Fatal(found, err)
	}
}

func TestTemporalReceiptFailureRollsBackProgress(t *testing.T) {
	h, in := temporalPending(t, "test-ok")
	ctx := context.Background()
	j, cached, err := h.jobs.TemporalClaim(ctx, in)
	if err != nil || cached != nil {
		t.Fatal(cached, err)
	}
	next, err := h.worker.Handler.Handle(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	// A real database rejection after the progress UPDATE must roll back both
	// the ordinal and lease release. No production fault-injection hook exists.
	_, err = pool.Exec(ctx, `CREATE FUNCTION checkout.test_receipt_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected receipt failure'; END $$; CREATE TRIGGER test_receipt_failure BEFORE INSERT ON checkout.temporal_steps FOR EACH ROW EXECUTE FUNCTION checkout.test_receipt_failure()`)
	if err != nil {
		t.Fatal(err)
	}
	drop := func() {
		if _, err := pool.Exec(ctx, `DROP TRIGGER IF EXISTS test_receipt_failure ON checkout.temporal_steps; DROP FUNCTION IF EXISTS checkout.test_receipt_failure()`); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(drop)
	if _, err = h.jobs.TemporalSave(ctx, j, in.Step, next, false); err == nil {
		t.Fatal("injected receipt failure did not fail")
	}
	var step, token int64
	var stage string
	var live bool
	if err = pool.QueryRow(ctx, `SELECT temporal_step,lease_token,status,lease_until>clock_timestamp() FROM checkout.jobs WHERE tenant_id=$1 AND id=$2::uuid`, in.TenantID, in.ID).Scan(&step, &token, &stage, &live); err != nil || step != 0 || token != j.Token || stage != domain.Reserving || !live {
		t.Fatal("receipt failure committed progress", step, token, stage, live, err)
	}
	drop()
	if _, err = h.jobs.TemporalSave(ctx, j, in.Step, next, false); err != nil {
		t.Fatal(err)
	}
}
