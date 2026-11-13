package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBudgetIsValidatedAndCannotExtendParent(t *testing.T) {
	for _, v := range []string{"0", "-1", "10001", "1.5", "1,2", "garbage"} {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Request-Timeout-Ms", v)
		w := httptest.NewRecorder()
		Guard(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("invalid budget reached use case") }), nil).ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("%q: %d", v, w.Code)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	parent, _ := ctx.Deadline()
	req := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	req.Header.Set("X-Request-Timeout-Ms", "10000")
	Guard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadline, ok := r.Context().Deadline()
		if !ok || deadline.After(parent) {
			t.Fatal("budget extended")
		}
		w.WriteHeader(204)
	}), nil).ServeHTTP(httptest.NewRecorder(), req)
}
