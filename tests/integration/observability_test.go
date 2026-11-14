//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	orders "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	catdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	catdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/quote"
	checkhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/httpapi"
	checkdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/advance"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/start"
	checkrun "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/runtime"
	invdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	orderdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	orderdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/features/placeorder"
	paydb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/provider"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/telemetry"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/telemetry/otlpfixture"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func observedRuntime(t *testing.T, service, endpoint string) *telemetry.Runtime {
	t.Helper()
	r, err := telemetry.New(telemetry.Options{Enabled: true, Durable: true, Service: service, Version: "development", Endpoint: endpoint, Sample: 1, Token: "metrics-fixture-only-key-at-least-32-characters"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}
func observedPool(t *testing.T, r *telemetry.Runtime) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config().Copy()
	cfg.ConnConfig.Tracer = telemetry.DBTracer{Runtime: r}
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
func saveOperationsReport(t *testing.T, part string, value any) {
	t.Helper()
	base := os.Getenv("ATLAS_OPERATIONS_REPORT_DIR")
	if base == "" {
		return
	}
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatal(err)
	}
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(base, part+".json"), append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestObservabilityCheckoutTraceAndStableIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	receiver := otlpfixture.New(4096)
	collector := httptest.NewServer(receiver)
	defer collector.Close()
	apiRuntime := observedRuntime(t, "api", collector.URL)
	workerRuntime := observedRuntime(t, "worker", collector.URL)
	providerRuntime := observedRuntime(t, "provider", collector.URL)
	apiPool, workerPool, providerPool := observedPool(t, apiRuntime), observedPool(t, workerRuntime), observedPool(t, providerRuntime)
	server := httptest.NewServer(providerRuntime.HTTP(provider.Server{Pool: providerPool, Token: providerTestToken}.Handler()))
	defer server.Close()
	client, err := provider.NewClient(server.URL, providerTestToken)
	if err != nil {
		t.Fatal(err)
	}
	catalog := &catdb.Store{Pool: apiPool}
	apiOrders := &orderdb.Store{Pool: apiPool}
	workerOrders := &orderdb.Store{Pool: workerPool}
	jobs := &checkdb.Store{Pool: workerPool}
	h := &commerce{catalog: catalog, orders: apiOrders, jobs: jobs}
	h.worker = checkrun.Worker{Store: jobs, Handler: advance.Handler{Inventory: invdb.Workflow{Store: &invdb.Store{Pool: workerPool}}, Payments: &paydb.Service{Pool: workerPool, Provider: client}, Orders: orderdb.Checkout{Store: workerOrders}}}
	id := commerceTenant(t)
	receive(t, id, "PRIVATE-SKU-OBS", "stock", 10)
	product(t, h, id, "PRIVATE-SKU-OBS", "price", 1200, 0)
	frozen, _, err := (quote.Handler{Store: catalog}).Handle(ctx, id, "quote", quote.Command{Lines: []catdomain.Line{{SKU: "PRIVATE-SKU-OBS", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := (placeorder.Handler{Store: apiOrders}).Handle(ctx, id, "order", placeorder.Command{Lines: []orderdomain.Line{{SKU: "PRIVATE-SKU-OBS", Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	keys := map[[32]byte]string{sha256.Sum256([]byte(rpcToken)): id}
	handler := apiRuntime.HTTP(checkhttp.New(start.Handler{Catalog: catalog, Orders: orderdb.Checkout{Store: apiOrders}}, jobs, keys))
	body, _ := json.Marshal(map[string]any{"quote_id": frozen.ID, "expected_version": 1, "payment_method": "test-ok"})
	request := func(parent string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/v1/orders/"+state.ID+"/checkout", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+rpcToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "stable-observed-command")
		req.Header.Set("Traceparent", parent)
		req.Header.Set("Baggage", "customer=PRIVATE-CUSTOMER")
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, req)
		return out
	}
	first := request("00-11111111111111111111111111111111-2222222222222222-01")
	if first.Code != 202 {
		t.Fatal(first.Code, first.Body.String())
	}
	second := request("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
	if second.Code != 202 || second.Header().Get("Idempotent-Replay") != "true" || first.Body.String() != second.Body.String() {
		t.Fatal("trace changed command identity", second.Code, second.Body.String())
	}
	var checkout orders.View
	if err = json.Unmarshal(first.Body.Bytes(), &checkout); err != nil {
		t.Fatal(err)
	}
	if sent, err := workerOrders.DispatchOne(workerRuntime.Context(ctx), jobs); err != nil || !sent {
		t.Fatal(sent, err)
	}
	workCtx := workerRuntime.Context(ctx)
	for i := 0; i < 20; i++ {
		view, err := jobs.Get(ctx, id, checkout.ID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Status == "confirmed" {
			break
		}
		if _, err = h.worker.ProcessOne(workCtx); err != nil {
			t.Fatal(err)
		}
	}
	final, err := jobs.Get(ctx, id, checkout.ID)
	if err != nil || final.Status != "confirmed" {
		t.Fatal(final, err)
	}
	if !envelope.ValidTraceParent(final.Metadata.TraceParent) {
		t.Fatal("trace parent did not survive SQL delivery", final.Metadata)
	}
	var events int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ordering.events WHERE tenant_id=$1 AND order_id=$2::uuid AND event_type=$3`, id, state.ID, orderdomain.CheckoutStarted).Scan(&events); err != nil || events != 1 {
		t.Fatal("trace retry duplicated event", events, err)
	}
	for _, runtime := range []*telemetry.Runtime{apiRuntime, workerRuntime, providerRuntime} {
		if err = runtime.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	records, rejected := receiver.Snapshot()
	if rejected != 0 {
		t.Fatal("fixture bounds exceeded", rejected)
	}
	names := map[string]int{}
	services := map[string]bool{}
	spans := map[string]otlpfixture.Record{}
	for _, record := range records {
		if hex.EncodeToString(record.Span.TraceId) == "11111111111111111111111111111111" {
			names[record.Span.Name]++
			services[record.Service()] = true
			spans[hex.EncodeToString(record.Span.SpanId)] = record
		}
	}
	for _, name := range []string{"http.request", "ordering.checkout.publish", "messaging.consume", "checkout.step", "payment_provider.request", "sql.query"} {
		if names[name] == 0 {
			t.Fatal("missing native span boundary", name, names)
		}
	}
	for _, service := range []string{"atlas-api", "atlas-worker", "atlas-provider"} {
		if !services[service] {
			t.Fatal("missing independent runtime", service, services)
		}
	}
	parent := telemetry.ParseParent(final.Metadata.TraceParent)
	origin, ok := spans[parent.SpanID().String()]
	if !ok || origin.Span.Name != "ordering.checkout.publish" {
		t.Fatal("persisted context points at no real producer span")
	}
	for _, record := range spans {
		if record.Span.Name == "checkout.step" && hex.EncodeToString(record.Span.ParentSpanId) != parent.SpanID().String() {
			t.Fatal("workflow lost durable parent")
		}
		if record.Service() == "atlas-provider" && record.Span.Name == "http.request" {
			up, ok := spans[hex.EncodeToString(record.Span.ParentSpanId)]
			if !ok || up.Span.Name != "payment_provider.request" {
				t.Fatal("provider did not inherit client parent")
			}
		}
	}
	encoded, _ := json.Marshal(records)
	for _, private := range []string{id, state.ID, checkout.ID, rpcToken, providerTestToken, "PRIVATE-SKU-OBS", "PRIVATE-CUSTOMER", "unit_price_minor", "SELECT ", "INSERT "} {
		if bytes.Contains(encoded, []byte(private)) {
			t.Fatal("private data reached actual OTLP payload", private)
		}
	}
	saveOperationsReport(t, "checkout-trace", map[string]any{"trace_id": "11111111111111111111111111111111", "spans_by_name": names, "services": services, "persisted_parent_is_real_producer": true, "same_command_after_changed_trace": "one event, same saved response", "confirmed": final.Status, "quote_total_minor": frozen.TotalMinor, "captured_records": len(records), "fixture_rejected": rejected, "payload_privacy": "checked credentials, subject/resource IDs, values and SQL text"})
}
func metricValue(t *testing.T, r *telemetry.Runtime, name string) float64 {
	t.Helper()
	families, err := r.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.Metric) == 1 {
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatal("metric missing", name)
	return 0
}
func TestObservabilityFreshnessAndIntegrityAreIndependent(t *testing.T) {
	ctx := context.Background()
	id := tenant(t)
	receive(t, id, "OBS-AUDIT", "seed", 10)
	runtime, err := telemetry.New(telemetry.Options{Service: "audit", Version: "development", Sample: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(ctx)
	audit := telemetry.Audit{Runtime: runtime, Pool: pool}
	if err = audit.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	if metricValue(t, runtime, "atlas_consumer_pending") < 1 || metricValue(t, runtime, "atlas_integrity_mismatches") != 0 {
		t.Fatal("lag was misclassified as corruption")
	}
	for range 1000 {
		worked, err := store.ProcessOne(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	if err = audit.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	if metricValue(t, runtime, "atlas_integrity_mismatches") != 0 {
		t.Fatal("matching view flagged")
	}
	if _, err = pool.Exec(ctx, `UPDATE inventory.stock_views SET on_hand=9 WHERE tenant_id=$1 AND sku='OBS-AUDIT'`, id); err != nil {
		t.Fatal(err)
	}
	if err = audit.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	if metricValue(t, runtime, "atlas_integrity_mismatches") != 1 {
		t.Fatal("same-version drift was invisible")
	}
	if _, err = pool.Exec(ctx, `UPDATE inventory.stock_views v SET on_hand=s.on_hand FROM inventory.stock s WHERE v.tenant_id=s.tenant_id AND v.sku=s.sku AND v.warehouse_id=s.warehouse_id AND v.tenant_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err = audit.Observe(ctx); err != nil || metricValue(t, runtime, "atlas_integrity_mismatches") != 0 {
		t.Fatal("fixture repair did not clear integrity signal", err)
	}
	saveOperationsReport(t, "audits", map[string]any{"unprocessed_source_is_lag": true, "same_version_value_drift_detected": 1, "after_fixture_repair": 0, "snapshot": "one SQL statement"})
}
func TestObservabilityMigrationSerializationAndMetadataExpansion(t *testing.T) {
	ctx := context.Background()
	held, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback(ctx)
	if _, err = held.Exec(ctx, `SELECT pg_advisory_xact_lock(7392018501)`); err != nil {
		t.Fatal(err)
	}
	timeout, stop := context.WithTimeout(ctx, 80*time.Millisecond)
	defer stop()
	if err = migrations.Apply(timeout, pool); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked migration ignored caller budget", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			errs <- migrations.Apply(job, pool)
		}()
	}
	waitRPC(t, func() bool {
		var waiting int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock%'`).Scan(&waiting)
		return err == nil && waiting >= 2
	})
	if err = held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = migrations.Check(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM atlas_schema_migrations WHERE name='000_platform/002_trace_context.sql'`).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	meta := envelope.Metadata{Version: 1, Actor: "system:internal", CorrelationID: "33333333-3333-4333-8333-333333333333", CausationID: "33333333-3333-4333-8333-333333333333", TraceParent: "00-11111111111111111111111111111111-2222222222222222-01"}
	for _, valid := range []bool{true, false} {
		candidate := meta
		if !valid {
			candidate.TraceParent = strings.Replace(candidate.TraceParent, "-01", "-03", 1)
		}
		b, _ := json.Marshal(candidate)
		var accepted bool
		if err = pool.QueryRow(ctx, `SELECT platform.valid_metadata($1::jsonb)`, b).Scan(&accepted); err != nil || accepted != valid {
			t.Fatal("SQL/Go trace validation differs", accepted, err)
		}
	}
	legacy := meta
	legacy.TraceParent = ""
	if !legacy.Valid() {
		t.Fatal("reader expansion broke legacy lineage")
	}
	saveOperationsReport(t, "migration-serialization", map[string]any{"blocked_jobs_observed": 2, "canceled_lock_wait": true, "concurrent_jobs_completed": 2, "expansion_records": n, "legacy_metadata_valid": true, "invalid_trace_rejected": true})
}
