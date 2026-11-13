package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
)

type sourceStub struct {
	Source
	mu       sync.Mutex
	products map[string]domain.Product
	calls    atomic.Int64
	gate     <-chan struct{}
	entered  chan struct{}
	once     sync.Once
	fail     error
}

func product(version int64) domain.Product {
	return domain.Product{SKU: "BOOK", Name: "Catalog book", UnitPriceMinor: 100, Currency: "USD", Version: version}
}
func (s *sourceStub) Product(ctx context.Context, tenant, sku string) (domain.Product, error) {
	s.calls.Add(1)
	s.mu.Lock()
	p, found := s.products[tenant+"/"+sku]
	gate, entered, fail := s.gate, s.entered, s.fail
	s.mu.Unlock()
	if entered != nil {
		s.once.Do(func() { close(entered) })
	}
	if gate != nil {
		select {
		case <-ctx.Done():
			return domain.Product{}, ctx.Err()
		case <-gate:
		}
	}
	if fail != nil {
		return domain.Product{}, fail
	}
	if !found {
		return domain.Product{}, api.ErrNotFound
	}
	return p, nil
}
func (s *sourceStub) Publish(_ context.Context, r commands.Request, p domain.Product) (domain.Product, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.products[r.Tenant+"/"+p.SKU]
	if current.Version != p.Version {
		return domain.Product{}, false, api.ErrConflict
	}
	p.Version++
	s.products[r.Tenant+"/"+p.SKU] = p
	return p, false, nil
}
func fixture(t *testing.T) (*Store, *sourceStub, *Memory) {
	t.Helper()
	source := &sourceStub{products: map[string]domain.Product{"acme/BOOK": product(1)}}
	memory, _ := NewMemory(64)
	store, e := New(source, memory, Defaults())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e = store.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	return store, source, memory
}
func until(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cache checkpoint did not converge")
}
func TestCacheAsideNegativeTenantScopeAndWriteFence(t *testing.T) {
	store, source, _ := fixture(t)
	ctx := context.Background()
	first, state, e := store.CachedProduct(ctx, "acme", "BOOK", 0)
	if e != nil || state != "miss" || first.Version != 1 {
		t.Fatal(first, state, e)
	}
	second, state, e := store.CachedProduct(ctx, "acme", "BOOK", 0)
	if e != nil || state != "hit" || second != first || source.calls.Load() != 1 {
		t.Fatal(second, state, e, source.calls.Load())
	}
	if _, _, e = store.CachedProduct(ctx, "other", "BOOK", 0); !errors.Is(e, api.ErrNotFound) {
		t.Fatal("tenant escaped:", e)
	}
	_, state, e = store.CachedProduct(ctx, "other", "BOOK", 0)
	if !errors.Is(e, api.ErrNotFound) || state != "negative" || source.calls.Load() != 2 {
		t.Fatal(state, e, source.calls.Load())
	}
	r, _ := commands.New("other", "publish-price", "create", nil)
	p := product(0)
	out, _, e := store.Publish(ctx, r, p)
	if e != nil || out.Version != 1 {
		t.Fatal(out, e)
	}
	out, _, e = store.CachedProduct(ctx, "other", "BOOK", 0)
	if e != nil || out.Version != 1 {
		t.Fatal("negative entry survived local creation", out, e)
	}
	r, _ = commands.New("acme", "publish-price", "update", nil)
	p = product(1)
	p.Name = "new"
	if _, _, e = store.Publish(ctx, r, p); e != nil {
		t.Fatal(e)
	}
	out, _, e = store.CachedProduct(ctx, "acme", "BOOK", 2)
	if e != nil || out.Version != 2 || out.Name != "new" {
		t.Fatal(out, e)
	}
}
func TestHotKeySingleflightAndOneCanceledCaller(t *testing.T) {
	store, source, _ := fixture(t)
	release := make(chan struct{})
	source.gate = release
	source.entered = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, _, e := store.CachedProduct(ctx, "acme", "BOOK", 0); first <- e }()
	<-source.entered
	results := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, _, e := store.CachedProduct(context.Background(), "acme", "BOOK", 0)
			if e == nil && out.Version != 1 {
				e = errors.New("wrong stock version")
			}
			results <- e
		}()
	}
	until(t, func() bool { return store.Stats().Flights.Shared == 32 })
	cancel()
	if e := <-first; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	close(release)
	wg.Wait()
	close(results)
	for e := range results {
		if e != nil {
			t.Fatal(e)
		}
	}
	if source.calls.Load() != 1 || store.Stats().Flights.Active != 0 {
		t.Fatal("stampede or leaked load", source.calls.Load(), store.Stats())
	}
}
func TestInvalidationCancelsLateFillAndMinimumVersionBypassesOldValue(t *testing.T) {
	store, source, memory := fixture(t)
	release := make(chan struct{})
	source.gate = release
	source.entered = make(chan struct{})
	done := make(chan domain.Product, 1)
	failed := make(chan error, 1)
	go func() {
		p, _, e := store.CachedProduct(context.Background(), "acme", "BOOK", 0)
		done <- p
		failed <- e
	}()
	<-source.entered
	source.mu.Lock()
	source.gate = nil
	source.mu.Unlock()
	r, _ := commands.New("acme", "publish-price", "new-version", nil)
	if _, _, e := store.Publish(context.Background(), r, product(1)); e != nil {
		t.Fatal(e)
	}
	close(release)
	out, e := <-done, <-failed
	if e != nil || out.Version != 2 {
		t.Fatal("late fill resurrected old version", out, e)
	}
	value, e := memory.Get(context.Background(), key("acme", "BOOK"))
	if e != nil || value.Version != 2 || value.Floor != 2 {
		t.Fatal(value, e)
	}
	source.mu.Lock()
	source.products["acme/BOOK"] = product(3)
	source.mu.Unlock()
	out, _, e = store.CachedProduct(context.Background(), "acme", "BOOK", 3)
	if e != nil || out.Version != 3 {
		t.Fatal("minimum version served a stale cache hit", out, e)
	}
}

type outage struct {
	Backend
	down atomic.Bool
}

func (o *outage) Get(ctx context.Context, key string) (Snapshot, error) {
	if o.down.Load() {
		return Snapshot{}, ErrCache
	}
	return o.Backend.Get(ctx, key)
}
func (o *outage) Put(ctx context.Context, key string, v int64, b []byte, ttl, retention time.Duration) (bool, error) {
	if o.down.Load() {
		return false, ErrCache
	}
	return o.Backend.Put(ctx, key, v, b, ttl, retention)
}
func TestCacheOutageAndCorruptionFallBackWithoutFalseMisses(t *testing.T) {
	store, source, memory := fixture(t)
	bad := &outage{Backend: memory}
	store.backend = bad
	ctx := context.Background()
	if _, _, e := store.CachedProduct(ctx, "acme", "BOOK", 0); e != nil {
		t.Fatal(e)
	}
	source.mu.Lock()
	source.products["acme/BOOK"] = product(2)
	source.mu.Unlock()
	bad.down.Store(true)
	out, status, e := store.CachedProduct(ctx, "acme", "BOOK", 0)
	if e != nil || out.Version != 2 || status != "fallback" {
		t.Fatal(out, status, e)
	}
	// A corrupt or cross-tenant cache record is advisory data, never authority.
	fresh, _, m := fixture(t)
	body, _ := encode("other", "BOOK", product(1), false)
	if _, e = m.Put(ctx, key("acme", "BOOK"), 1, body, time.Second, time.Minute); e != nil {
		t.Fatal(e)
	}
	out, _, e = fresh.CachedProduct(ctx, "acme", "BOOK", 0)
	if e != nil || out.Version != 1 || fresh.Stats().Corrupt != 1 {
		t.Fatal(out, e, fresh.Stats())
	}
	source.mu.Lock()
	source.fail = errors.New("source unavailable")
	source.mu.Unlock()
	if _, _, e = store.CachedProduct(ctx, "acme", "BOOK", 0); e == nil || errors.Is(e, api.ErrNotFound) {
		t.Fatal("origin outage became negative cache entry", e)
	}
}
func TestMemoryBoundsTTLJitterAndExactVersionOrdering(t *testing.T) {
	store, _, _ := fixture(t)
	for i := 0; i < 100; i++ {
		ttl := store.ttl(time.Second)
		if ttl < 800*time.Millisecond || ttl > time.Second {
			t.Fatal(ttl)
		}
	}
	memory, _ := NewMemory(2)
	now := time.Now()
	memory.now = func() time.Time { return now }
	ctx := context.Background()
	version := int64(9_007_199_254_740_993)
	if e := memory.Invalidate(ctx, "key", version, time.Minute); e != nil {
		t.Fatal(e)
	}
	if ok, e := memory.Put(ctx, "key", version-1, []byte("old"), time.Second, time.Minute); e != nil || ok {
		t.Fatal(ok, e)
	}
	if ok, e := memory.Put(ctx, "key", version, []byte("current"), time.Second, time.Minute); e != nil || !ok {
		t.Fatal(ok, e)
	}
	now = now.Add(2 * time.Second)
	snapshot, e := memory.Get(ctx, "key")
	if e != nil || len(snapshot.Payload) != 0 || snapshot.Floor != version {
		t.Fatal(snapshot, e)
	}
	_, _ = memory.Put(ctx, "a", 1, []byte("a"), time.Second, time.Minute)
	_, _ = memory.Put(ctx, "b", 1, []byte("b"), time.Second, time.Minute)
	size, evictions := memory.Size()
	if size != 2 || evictions != 1 {
		t.Fatal(size, evictions)
	}
}

func TestReadThroughRefreshAheadAndWriteThroughComparison(t *testing.T) {
	source := &sourceStub{products: map[string]domain.Product{"acme/BOOK": product(1)}}
	memory, _ := NewMemory(8)
	o := Defaults()
	o.WriteThrough = true
	o.RefreshAhead = 150 * time.Millisecond
	o.JitterPercent = 0
	cached, err := New(source, memory, o)
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close(context.Background())
	r, _ := commands.New("acme", "publish-product", "p2", nil)
	p := product(1)
	out, _, err := cached.Publish(context.Background(), r, p)
	if err != nil {
		t.Fatal(err)
	}
	if got, state, err := cached.CachedProduct(context.Background(), "acme", "BOOK", 0); err != nil || state != "hit" || got != out || source.calls.Load() != 0 {
		t.Fatal("write through did not prime read", got, state, err)
	}
	// Age the real memory backend without a timing-sensitive wall-clock sleep.
	memory.mu.Lock()
	memory.entries[key("acme", "BOOK")].Value.(*memoryEntry).expires = time.Now().Add(100 * time.Millisecond)
	memory.mu.Unlock()
	source.mu.Lock()
	source.products["acme/BOOK"] = product(3)
	source.mu.Unlock()
	got, state, err := cached.CachedProduct(context.Background(), "acme", "BOOK", 0)
	if err != nil || state != "hit" || got.Version != 2 {
		t.Fatal("refresh blocked foreground hit", got, state, err)
	}
	until(t, func() bool { return cached.Stats().Flights.Active == 0 && source.calls.Load() == 1 })
	got, state, err = cached.CachedProduct(context.Background(), "acme", "BOOK", 0)
	if err != nil || state != "hit" || got.Version != 3 || cached.Stats().Refreshes != 1 {
		t.Fatal(got, state, err, cached.Stats())
	}
}
