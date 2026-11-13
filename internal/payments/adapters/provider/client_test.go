package provider

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
)

func TestProviderPermanentErrorsAndWorkflowMutationRetryOwnership(t *testing.T) {
	var calls atomic.Int64
	var status atomic.Int64
	status.Store(404)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(int(status.Load())) }))
	defer server.Close()
	client, err := NewClient(server.URL, "provider-fixture-key-at-least-32-characters")
	if err != nil {
		t.Fatal(err)
	}
	intent := api.Intent{ID: "00000000-0000-4000-8000-000000000001", TenantID: "acme", AmountMinor: 1000, Currency: "USD", Method: "test-ok"}
	if _, err = client.Get(context.Background(), intent); !errors.Is(err, api.ErrUnknown) || calls.Load() != 1 {
		t.Fatal("permanent rejection retried", err, calls.Load())
	}
	status.Store(503)
	if _, err = client.Capture(context.Background(), intent); !errors.Is(err, api.ErrUnknown) || calls.Load() != 2 {
		t.Fatal("workflow mutation acquired nested retries", err, calls.Load())
	}
	if stats := client.Policy.Stats(); stats.Budget.Retries != 0 || stats.Bulkhead.Active != 0 {
		t.Fatal(stats)
	}
}
func TestProviderParentCancellationReleasesAttempt(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "provider-fixture-key-at-least-32-characters")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.Get(ctx, api.Intent{ID: "00000000-0000-4000-8000-000000000001"}); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request never reached provider")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, api.ErrUnknown) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation retained permit")
	}
	if stats := client.Policy.Stats(); stats.Physical != 1 || stats.Bulkhead.Active != 0 || stats.Budget.Retries != 0 {
		t.Fatal(stats)
	}
}
