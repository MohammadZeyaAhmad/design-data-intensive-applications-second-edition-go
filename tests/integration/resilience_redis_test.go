//go:build integration && redis

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	catcache "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/cache"
	catdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

func nativeRedis(t *testing.T) (*catcache.Redis, string) {
	t.Helper()
	address := os.Getenv("ATLAS_TEST_REDIS_ADDRESS")
	if address == "" {
		address = "127.0.0.1:16380"
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil || (host != "127.0.0.1" && host != "localhost") {
		t.Fatal("native cache tests require a loopback Redis listener")
	}
	id, err := identity.NewID()
	if err != nil {
		t.Fatal(err)
	}
	prefix := "atlas:test:" + id
	backend, err := catcache.NewRedis(address, "", prefix)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = backend.Client.Ping(ctx).Err(); err != nil {
		t.Fatal("Redis is required; no silent service skip", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		defer backend.Close()
		var cursor uint64
		for {
			keys, next, e := backend.Client.Scan(ctx, cursor, prefix+":*", 64).Result()
			if e != nil {
				t.Error(e)
				return
			}
			if len(keys) > 0 {
				if e = backend.Client.Del(ctx, keys...).Err(); e != nil {
					t.Error(e)
					return
				}
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
	return backend, prefix
}
func TestResilienceRedisExactVersionsAndLateFillFences(t *testing.T) {
	backend, _ := nativeRedis(t)
	ctx := context.Background()
	const version int64 = 9007199254740993
	if ok, err := backend.Put(ctx, "key", version-1, []byte("old"), time.Second, 5*time.Second); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := backend.Invalidate(ctx, "key", version, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if ok, err := backend.Put(ctx, "key", version-1, []byte("late"), time.Second, 5*time.Second); err != nil || ok {
		t.Fatal("stale fill crossed exact int64 fence", ok, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := backend.Put(ctx, "key", version+int64(i), []byte(fmt.Sprint(i)), time.Second, 5*time.Second)
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
	out, err := backend.Get(ctx, "key")
	if err != nil || out.Floor != version || out.Version != version+15 || string(out.Payload) != "15" {
		t.Fatal(out, err)
	}
	if report := os.Getenv("ATLAS_RESILIENCE_REDIS_REPORT"); report != "" {
		info, err := backend.Client.Info(ctx, "server").Result()
		if err != nil {
			t.Fatal(err)
		}
		runtime := ""
		for _, line := range strings.Split(info, "\n") {
			if strings.HasPrefix(line, "redis_version:") {
				runtime = strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
			}
		}
		if runtime != "7.4.2" {
			t.Fatal("unexpected Redis runtime", runtime)
		}
		if err = os.MkdirAll(filepath.Dir(report), 0755); err != nil {
			t.Fatal(err)
		}
		body, _ := json.MarshalIndent(map[string]any{"runtime": runtime, "fence": out.Floor, "max_concurrent_version": out.Version, "late_fill_rejected": true, "writers": 16, "sdk_retries": 0}, "", "  ")
		if err = os.WriteFile(report, append(body, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
func TestResilienceRedisExpiryAndCorruptValueBounds(t *testing.T) {
	backend, prefix := nativeRedis(t)
	ctx := context.Background()
	if ok, err := backend.Put(ctx, "ttl", 17, []byte("value"), 20*time.Millisecond, 5*time.Second); err != nil || !ok {
		t.Fatal(ok, err)
	}
	waitRPC(t, func() bool { snapshot, err := backend.Get(ctx, "ttl"); return err == nil && len(snapshot.Payload) == 0 })
	snapshot, err := backend.Get(ctx, "ttl")
	if err != nil || snapshot.Version != 17 {
		t.Fatal("expiry removed advisory version prematurely", snapshot, err)
	}
	if err = backend.Invalidate(ctx, "ttl", 18, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if ok, err := backend.Put(ctx, "ttl", 17, []byte("late"), time.Second, 5*time.Second); err != nil || ok {
		t.Fatal(ok, err)
	}
	if err = backend.Client.HSet(ctx, prefix+":large", "payload", strings.Repeat("x", 64<<10), "expires", time.Now().Add(time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	snapshot, err = backend.Get(ctx, "large")
	if err != nil || len(snapshot.Payload) != 0 {
		t.Fatal("oversized cache payload crossed adapter", len(snapshot.Payload), err)
	}
	if err = backend.Client.HSet(ctx, prefix+":broken", "floor", "not-an-int64").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = backend.Get(ctx, "broken"); err == nil {
		t.Fatal("corrupt metadata was trusted")
	}
}
func TestResilienceRedisCatalogNegativeTenantAndWriteThrough(t *testing.T) {
	backend, _ := nativeRedis(t)
	raw := &catdb.Store{Pool: pool}
	source := &measuredCatalog{Store: raw}
	ctx := context.Background()
	id := tenant(t)
	other := tenant(t)
	resiliencePrice(t, raw, id, "seed", 0, 1200)
	resiliencePrice(t, raw, other, "seed", 0, 987)
	o := catcache.Defaults()
	o.WriteThrough = true
	cached, err := catcache.New(source, backend, o)
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close(ctx)
	for tenant, price := range map[string]int64{id: 1200, other: 987} {
		p, _, err := cached.CachedProduct(ctx, tenant, "CACHE", 0)
		if err != nil || p.UnitPriceMinor != price {
			t.Fatal(p, err)
		}
	}
	if source.calls.Load() != 2 {
		t.Fatal(source.calls.Load())
	}
	_, state, err := cached.CachedProduct(ctx, id, "CACHE", 0)
	if err != nil || state != "hit" || source.calls.Load() != 2 {
		t.Fatal(state, err)
	}
	_, _, err = cached.CachedProduct(ctx, id, "MISSING", 0)
	if err != cat.ErrNotFound {
		t.Fatal(err)
	}
	_, state, err = cached.CachedProduct(ctx, id, "MISSING", 0)
	if err != cat.ErrNotFound || state != "negative" {
		t.Fatal(state, err)
	}
	newPrice := resiliencePrice(t, cached, id, "write-through", 1, 2400)
	before := source.calls.Load()
	p, state, err := cached.CachedProduct(ctx, id, "CACHE", 0)
	if err != nil || p != newPrice || state != "hit" || source.calls.Load() != before {
		t.Fatal(p, state, err)
	}
}
