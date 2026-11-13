//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	pay "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
	catcache "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/cache"
	cathttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/httpapi"
	catdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	catdomain "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/publish"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/quote"
	invhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/httpapi"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/provider"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/resilience"
)

type measuredCatalog struct {
	*catdb.Store
	calls atomic.Int64
	gate  <-chan struct{}
}

func (s *measuredCatalog) Product(ctx context.Context, tenant, sku string) (catdomain.Product, error) {
	s.calls.Add(1)
	p, err := s.Store.Product(ctx, tenant, sku)
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return catdomain.Product{}, ctx.Err()
		}
	}
	return p, err
}
func resilienceCache(t *testing.T, source catcache.Source) *catcache.Store {
	t.Helper()
	memory, _ := catcache.NewMemory(64)
	cached, err := catcache.New(source, memory, catcache.Defaults())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := cached.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return cached
}
func resiliencePrice(t *testing.T, source catcache.Source, tenant, key string, version, price int64) catdomain.Product {
	t.Helper()
	p, _, err := (publish.Handler{Store: source}).Handle(context.Background(), tenant, key, catdomain.Product{SKU: "CACHE", Name: "Book", Currency: "USD", UnitPriceMinor: price, Version: version})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestResilienceCatalogCacheQuotesAndCoalescing(t *testing.T) {
	ctx := context.Background()
	id := tenant(t)
	other := tenant(t)
	raw := &catdb.Store{Pool: pool}
	resiliencePrice(t, raw, id, "seed", 0, 1200)
	resiliencePrice(t, raw, other, "seed", 0, 777)
	gate := make(chan struct{})
	source := &measuredCatalog{Store: raw, gate: gate}
	cached := resilienceCache(t, source)
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _, err := cached.CachedProduct(ctx, id, "CACHE", 0)
			if err == nil && p.Version != 1 {
				err = errors.New("wrong coalesced version")
			}
			errs <- err
		}()
	}
	waitRPC(t, func() bool { return cached.Stats().Flights.Shared == 23 })
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if source.calls.Load() != 1 {
		t.Fatal("hot key caused repeated SQL reads", source.calls.Load())
	}
	// An intentionally disconnected display cache can lag a committed price.
	resiliencePrice(t, raw, id, "new-price", 1, 2300)
	old, _, err := cached.CachedProduct(ctx, id, "CACHE", 0)
	if err != nil || old.Version != 1 {
		t.Fatal(old, err)
	}
	current, err := cached.Product(ctx, id, "CACHE")
	if err != nil || current.Version != 2 {
		t.Fatal(current, err)
	}
	q, _, err := (quote.Handler{Store: cached}).Handle(ctx, id, "quote", quote.Command{Lines: []catdomain.Line{{SKU: "CACHE", Quantity: 2}}})
	if err != nil || q.TotalMinor != 4600 || q.Lines[0].PriceVersion != 2 {
		t.Fatal("stale display price entered quote", q, err)
	}
	p, _, err := cached.CachedProduct(ctx, id, "CACHE", 2)
	if err != nil || p.Version != 2 {
		t.Fatal("minimum version was ignored", p, err)
	}
	p, _, err = cached.CachedProduct(ctx, other, "CACHE", 0)
	if err != nil || p.UnitPriceMinor != 777 {
		t.Fatal("cache escaped tenant", p, err)
	}
	keys := map[[32]byte]string{sha256.Sum256([]byte(rpcToken)): id}
	handler := cathttp.New(cached, keys)
	for _, path := range []string{"/v1/catalog/products/CACHE", "/v1/catalog/products/CACHE?consistency=cached&min_version=2"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+rpcToken)
		handler.ServeHTTP(w, r)
		if w.Code != 200 || w.Header().Get("X-Consistency") == "" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if _, _, err = cached.CachedProduct(ctx, id, "ABSENT", 0); !errors.Is(err, cat.ErrNotFound) {
		t.Fatal(err)
	}
	_, state, err := cached.CachedProduct(ctx, id, "ABSENT", 0)
	if !errors.Is(err, cat.ErrNotFound) || state != "negative" {
		t.Fatal(state, err)
	}
	if report := os.Getenv("ATLAS_RESILIENCE_REPORT"); report != "" {
		if err = os.MkdirAll(filepath.Dir(report), 0755); err != nil {
			t.Fatal(err)
		}
		b, _ := json.MarshalIndent(map[string]any{"hot_key_waiters": 24, "hot_key_sql_reads": 1, "stale_display_version": old.Version, "authoritative_version": current.Version, "quote_total_minor": q.TotalMinor, "quote_price_version": q.Lines[0].PriceVersion, "cache": cached.Stats()}, "", "  ")
		if err = os.WriteFile(report, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
func TestResilienceTransactionalInvalidationAcrossInstances(t *testing.T) {
	ctx := context.Background()
	id := tenant(t)
	raw := &catdb.Store{Pool: pool}
	resiliencePrice(t, raw, id, "seed", 0, 1200)
	first, second := resilienceCache(t, raw), resilienceCache(t, raw)
	dsn := pool.Config().ConnString()
	l1, l2 := catcache.StartListener(ctx, dsn, first), catcache.StartListener(ctx, dsn, second)
	defer l1.Close(ctx)
	defer l2.Close(ctx)
	for _, l := range []*catcache.Listener{l1, l2} {
		select {
		case <-l.Ready():
		case <-time.After(2 * time.Second):
			t.Fatal("LISTEN did not start")
		}
	}
	for _, s := range []*catcache.Store{first, second} {
		if _, _, err := s.CachedProduct(ctx, id, "CACHE", 0); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE catalog.products SET version=2 WHERE tenant_id=$1 AND sku='CACHE'`, id); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A committed notification after rollback acts as an ordered listener barrier.
	barrier, _ := json.Marshal(map[string]any{"tenant": id, "sku": "BARRIER", "version": 1})
	if _, err = pool.Exec(ctx, `SELECT pg_notify('atlas_catalog_cache_v1',$1)`, string(barrier)); err != nil {
		t.Fatal(err)
	}
	waitRPC(t, func() bool { a, _ := l1.Counts(); b, _ := l2.Counts(); return a >= 1 && b >= 1 })
	for _, s := range []*catcache.Store{first, second} {
		p, state, e := s.CachedProduct(ctx, id, "CACHE", 0)
		if e != nil || state != "hit" || p.Version != 1 || s.Stats().Invalidations != 1 {
			t.Fatal("rollback invalidated cache", p, state, e, s.Stats())
		}
	}
	resiliencePrice(t, raw, id, "commit", 1, 2500)
	waitRPC(t, func() bool { a, _ := l1.Counts(); b, _ := l2.Counts(); return a >= 2 && b >= 2 })
	for _, s := range []*catcache.Store{first, second} {
		p, _, e := s.CachedProduct(ctx, id, "CACHE", 0)
		if e != nil || p.Version != 2 || p.UnitPriceMinor != 2500 {
			t.Fatal("committed invalidation was lost", p, e)
		}
	}
}
func TestResilienceHTTPAdmissionCancellationAndDrain(t *testing.T) {
	ctx := context.Background()
	id := tenant(t)
	receive(t, id, "ADMISSION", "seed", 10)
	o := resilience.DefaultHTTPOptions()
	o.WriteActive = 1
	o.WriteQueued = 1
	o.ReadActive = 1
	o.ReadQueued = 0
	admission, _ := resilience.NewHTTPAdmission(o)
	keys := map[[32]byte]string{sha256.Sum256([]byte(rpcToken)): id}
	app := invhttp.New(invhttp.Options{Store: store, TenantKeys: keys, Ready: func(context.Context) error { return nil }})
	server := httptest.NewServer(httpx.Guard(admission.Handler(app), nil))
	defer server.Close()
	held, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback(ctx)
	if _, err = held.Exec(ctx, `SELECT 1 FROM inventory.stock WHERE tenant_id=$1 AND sku='ADMISSION' FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	request := func(ctx context.Context, method, path, key string) (int, error) {
		body := io.Reader(nil)
		if method == "POST" {
			body = strings.NewReader(`{"sku":"ADMISSION","quantity":1}`)
		}
		r, _ := http.NewRequestWithContext(ctx, method, server.URL+path, body)
		r.Header.Set("Authorization", "Bearer "+rpcToken)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("X-Request-Timeout-Ms", "4000")
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(r)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode, nil
	}
	type result struct {
		code int
		err  error
	}
	active := make(chan result, 1)
	queued := make(chan result, 1)
	go func() { code, err := request(ctx, "POST", "/v1/reservations", "accepted"); active <- result{code, err} }()
	waitRPC(t, func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'`).Scan(&n)
		return err == nil && n > 0
	})
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		code, err := request(canceled, "POST", "/v1/reservations", "canceled")
		queued <- result{code, err}
	}()
	waitRPC(t, func() bool { return admission.Writes.Stats().Queued == 1 })
	if code, err := request(ctx, "POST", "/v1/reservations", "rejected"); err != nil || code != 503 {
		t.Fatal("excess write admitted", code, err)
	}
	if code, err := request(ctx, "GET", "/v1/stock/ADMISSION", ""); err != nil || code != 200 {
		t.Fatal("blocked writes consumed read lane", code, err)
	}
	cancel()
	if r := <-queued; !errors.Is(r.err, context.Canceled) {
		t.Fatal(r)
	}
	waitRPC(t, func() bool { return admission.Writes.Stats().Queued == 0 })
	admission.BeginDrain()
	if code, err := request(ctx, "GET", "/readyz", ""); err != nil || code != 503 {
		t.Fatal(code, err)
	}
	if code, err := request(ctx, "POST", "/v1/reservations", "during-drain"); err != nil || code != 503 {
		t.Fatal(code, err)
	}
	if err = held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if r := <-active; r.err != nil || r.code != 201 {
		t.Fatal("accepted work lost on drain", r)
	}
	drained, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err = admission.Drain(drained); err != nil {
		t.Fatal(err)
	}
	if count(t, "reservations", id) != 1 || count(t, "command_results", id) != 2 {
		t.Fatal("canceled/rejected requests reached SQL business work")
	}
}
func TestResilienceProviderIdentityBudgetAndRecovery(t *testing.T) {
	ctx := context.Background()
	id := tenant(t)
	server := httptest.NewServer(provider.Server{Pool: pool, Token: providerTestToken}.Handler())
	defer server.Close()
	client, err := provider.NewClient(server.URL, providerTestToken)
	if err != nil {
		t.Fatal(err)
	}
	client.RetryMutations = true
	pid, _ := identity.NewID()
	intent := pay.Intent{TenantID: id, ID: pid, AmountMinor: 1500, Currency: "USD", Method: "test-lost-response"}
	receipt, err := client.Capture(ctx, intent)
	if err != nil || receipt.Status != "captured" {
		t.Fatal("same-ID lost-reply retry failed", receipt, err)
	}
	var effects int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM provider.operations WHERE tenant_id=$1 AND id=$2::uuid AND capture_response_lost AND status='captured'`, id, pid).Scan(&effects); err != nil || effects != 1 {
		t.Fatal("physical retries duplicated effects", effects, err)
	}
	if stats := client.Policy.Stats(); stats.Physical != 2 || stats.Budget.Retries != 1 || stats.Bulkhead.Active != 0 {
		t.Fatal(stats)
	}
	var failing atomic.Bool
	failing.Store(true)
	var calls atomic.Int64
	outage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if failing.Load() {
			w.WriteHeader(503)
			return
		}
		httpx.JSON(w, 200, receipt)
	}))
	defer outage.Close()
	client, err = provider.NewClient(outage.URL, providerTestToken)
	if err != nil {
		t.Fatal(err)
	}
	for range 30 {
		_, _ = client.Get(ctx, intent)
	}
	stats := client.Policy.Stats()
	if stats.Physical != calls.Load() || stats.Physical > stats.Budget.Originals+2+stats.Budget.Originals/5 || stats.Breaker.State != "open" || stats.Bulkhead.Active != 0 {
		t.Fatal("retry amplification escaped budget", stats, calls.Load())
	}
	failing.Store(false)
	waitRPC(t, func() bool { r, e := client.Get(ctx, intent); return e == nil && r.Status == "captured" })
	if client.Policy.Stats().Breaker.State != "closed" {
		t.Fatal("healthy dependency did not close circuit")
	}
}
