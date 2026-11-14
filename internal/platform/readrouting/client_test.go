package readrouting

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSessionClientKeepsWatermarkAndMissingWriteFence(t *testing.T) {
	n := 0
	writeMissing := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing bearer")
		}
		if n > 0 && r.Header.Get("X-Session-Token") != strconv.Itoa(n) {
			t.Error("client lost watermark")
		}
		if r.Method == "POST" {
			if r.Header.Get("Idempotency-Key") != "receipt" {
				t.Error("lost receipt key")
			}
			writeMissing = true
			w.Header().Set("X-Session-Status", "unavailable")
			w.WriteHeader(200)
			return
		}
		if (n == 0 || writeMissing) && r.Header.Get("X-Read-Consistency") != "primary" {
			t.Error("missing fence did not require primary")
		}
		if n > 0 && !writeMissing && r.Header.Get("X-Read-Consistency") != "replica" {
			t.Error("did not reuse session")
		}
		writeMissing = false
		n++
		w.Header().Set("X-Session-Token", strconv.Itoa(n))
		w.Header().Set("X-Session-Status", "issued")
		fmt.Fprint(w, "{}")
	}))
	defer server.Close()
	c, e := NewSessionClient(server.URL, "secret", server.Client())
	if e != nil {
		t.Fatal(e)
	}
	get := func() {
		r, e := c.Do(context.Background(), "GET", "/v1/stock/BOOK", nil)
		if e != nil {
			t.Error(e)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	get()
	r, e := c.Do(context.Background(), "POST", "/v1/stock/BOOK/receipts", strings.NewReader(`{"quantity":1}`), "receipt")
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	get()
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(get)
	}
	wg.Wait()
	if _, e = c.Do(context.Background(), "GET", "//other.example/path", nil); e == nil {
		t.Fatal("accepted authority switch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	c.gate <- struct{}{}
	if _, e = c.Do(ctx, "GET", "/v1/stock/BOOK", nil); e == nil {
		t.Fatal("queue ignored cancellation")
	}
	<-c.gate
}
