package resilience

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTenantBulkheadsBoundIdentityMemoryAndIsolateWork(t *testing.T) {
	tenants, _ := NewTenants(1, 0, 2)
	releaseA, err := tenants.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tenants.Acquire(context.Background(), "a"); !errors.Is(err, ErrOverloaded) {
		t.Fatal(err)
	}
	releaseB, err := tenants.Acquire(context.Background(), "b")
	if err != nil {
		t.Fatal("noisy tenant blocked independent work", err)
	}
	if _, err = tenants.Acquire(context.Background(), "c"); !errors.Is(err, ErrOverloaded) {
		t.Fatal(err)
	}
	releaseA()
	releaseA()
	releaseC, err := tenants.Acquire(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	releaseB()
	releaseC()
	if s := tenants.Stats(); s.Tenants != 0 || s.Peak != 2 || s.Rejected != 2 {
		t.Fatal(s)
	}
}
func TestHTTPRateBucketsAndDrainReadiness(t *testing.T) {
	o := DefaultHTTPOptions()
	o.Rate = 1
	o.Burst = 1
	a, _ := NewHTTPAdmission(o)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	handler := a.Handler(next)
	call := func(method, path string) int {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w.Code
	}
	if call("GET", "/x") != 204 || call("GET", "/x") != 429 || call("POST", "/x") != 204 {
		t.Fatal("workload rate buckets were not independent")
	}
	if call("GET", "/healthz") != 204 {
		t.Fatal("liveness was rate limited")
	}
	a.BeginDrain()
	if call("GET", "/readyz") != 503 {
		t.Fatal("draining process remained ready")
	}
	limiter, _ := NewRate(2, 1)
	clock := time.Now()
	limiter.now = func() time.Time { return clock }
	limiter.last = clock
	if !limiter.Allow() || limiter.Allow() {
		t.Fatal("burst not bounded")
	}
	clock = clock.Add(500 * time.Millisecond)
	if !limiter.Allow() {
		t.Fatal("rate did not refill")
	}
}
