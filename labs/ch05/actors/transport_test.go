package actors

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
)

func TestLocalAndRemoteTransportShareActorReferenceSemantics(t *testing.T) {
	r := runtimeFixture(t, DefaultOptions())
	const token = "actors-test-token-long-enough-for-local-only"
	server := httptest.NewServer(Handler(r, "acme", token))
	defer server.Close()
	remote, e := NewClient(server.URL, token)
	if e != nil {
		t.Fatal(e)
	}
	senders := []Sender{Local{Runtime: r}, remote}
	for _, sender := range senders {
		ref, e := sender.Resolve(context.Background(), testKey)
		if e != nil {
			t.Fatal(e)
		}
		raw := wire(t, "shared-identity", "receive.v1", testKey, 3)
		if _, e = sender.Ask(context.Background(), ref, raw); e != nil {
			t.Fatal(e)
		}
	}
	ref, e := remote.Resolve(context.Background(), testKey)
	if e != nil {
		t.Fatal(e)
	}
	out, e := remote.Ask(context.Background(), ref, wire(t, "read", "get.v1", testKey, 0))
	if e != nil || out.State.OnHand != 3 {
		t.Fatal(out, e)
	}
	ref.Generation++
	if _, e = remote.Tell(context.Background(), ref, wire(t, "stale", "receive.v1", testKey, 1)); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if _, e = remote.Resolve(context.Background(), Key{Tenant: "other", SKU: "BOOK"}); e == nil {
		t.Fatal("remote key escaped tenant binding")
	}
}
