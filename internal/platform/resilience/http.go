package resilience

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/httpx"
)

// Rate is a fixed-size token bucket for a named workload, not an unbounded map
// keyed by untrusted request headers.
type Rate struct {
	mu                       sync.Mutex
	perSecond, burst, tokens float64
	last                     time.Time
	now                      func() time.Time
	rejected                 int64
}

func NewRate(perSecond, burst int) (*Rate, error) {
	if perSecond < 1 || perSecond > 100000 || burst < 1 || burst > 100000 {
		return nil, errors.New("invalid request rate")
	}
	return &Rate{perSecond: float64(perSecond), burst: float64(burst), tokens: float64(burst), last: time.Now(), now: time.Now}, nil
}
func (r *Rate) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	elapsed := now.Sub(r.last).Seconds()
	if elapsed > 0 {
		r.tokens += elapsed * r.perSecond
		if r.tokens > r.burst {
			r.tokens = r.burst
		}
		r.last = now
	}
	if r.tokens < 1 {
		r.rejected++
		return false
	}
	r.tokens--
	return true
}

type HTTPOptions struct{ ReadActive, ReadQueued, WriteActive, WriteQueued, Rate, Burst int }

func DefaultHTTPOptions() HTTPOptions {
	return HTTPOptions{ReadActive: 16, ReadQueued: 32, WriteActive: 16, WriteQueued: 32, Rate: 2000, Burst: 512}
}

type HTTPAdmission struct {
	Reads, Writes         *Bulkhead
	readSlots, writeSlots *Bulkhead
	readRate, writeRate   *Rate
}

func NewHTTPAdmission(o HTTPOptions) (*HTTPAdmission, error) {
	if o.ReadActive+o.ReadQueued > 1024 || o.WriteActive+o.WriteQueued > 1024 {
		return nil, errors.New("HTTP admission exceeds body-buffer bound")
	}
	reads, e := NewBulkhead(o.ReadActive, o.ReadQueued)
	if e != nil {
		return nil, e
	}
	writes, e := NewBulkhead(o.WriteActive, o.WriteQueued)
	if e != nil {
		return nil, e
	}
	rr, e := NewRate(o.Rate, o.Burst)
	if e != nil {
		return nil, e
	}
	wr, e := NewRate(o.Rate, o.Burst)
	if e != nil {
		return nil, e
	}
	rs, _ := NewBulkhead(o.ReadActive+o.ReadQueued, 0)
	ws, _ := NewBulkhead(o.WriteActive+o.WriteQueued, 0)
	return &HTTPAdmission{Reads: reads, Writes: writes, readSlots: rs, writeSlots: ws, readRate: rr, writeRate: wr}, nil
}
func (a *HTTPAdmission) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/openapi/inventory.json" {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/readyz" {
			if a.Reads.Stats().Draining || a.Writes.Stats().Draining {
				httpx.Problem(w, 503, "draining")
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		limiter, rate, slots := a.Writes, a.writeRate, a.writeSlots
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			limiter, rate, slots = a.Reads, a.readRate, a.readSlots
		}
		if !rate.Allow() {
			w.Header().Set("Retry-After", "1")
			httpx.Problem(w, 429, "rate_limited")
			return
		}
		// Reserve memory before consuming a body. Reading to EOF lets net/http
		// observe an HTTP/1 peer disconnect while this request waits in the queue.
		// Every buffered request owns a slot until its handler returns.
		slot, e := slots.Acquire(r.Context())
		if e != nil {
			w.Header().Set("Retry-After", "1")
			httpx.Problem(w, 503, "overloaded_or_draining")
			return
		}
		defer slot.Release()
		if r.Body != nil && r.Body != http.NoBody {
			controller := http.NewResponseController(w)
			if deadline, ok := r.Context().Deadline(); ok {
				_ = controller.SetReadDeadline(deadline)
			}
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
			_ = r.Body.Close()
			_ = controller.SetReadDeadline(time.Time{})
			if err != nil {
				var oversized *http.MaxBytesError
				if errors.As(err, &oversized) {
					httpx.Problem(w, 413, "body_too_large")
				} else {
					httpx.Problem(w, 400, "invalid_body")
				}
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		permit, e := limiter.Acquire(r.Context())
		if e != nil {
			if errors.Is(e, context.DeadlineExceeded) {
				httpx.Problem(w, 504, "admission_deadline")
			} else if errors.Is(e, context.Canceled) {
				httpx.Problem(w, 408, "request_canceled")
			} else {
				w.Header().Set("Retry-After", "1")
				httpx.Problem(w, 503, "overloaded_or_draining")
			}
			return
		}
		defer permit.Release()
		next.ServeHTTP(w, r)
	})
}
func (a *HTTPAdmission) BeginDrain() {
	a.readSlots.BeginDrain()
	a.writeSlots.BeginDrain()
	a.Reads.BeginDrain()
	a.Writes.BeginDrain()
}
func (a *HTTPAdmission) Drain(ctx context.Context) error {
	a.BeginDrain()
	if e := a.Reads.Drain(ctx); e != nil {
		return e
	}
	if e := a.Writes.Drain(ctx); e != nil {
		return e
	}
	if e := a.readSlots.Drain(ctx); e != nil {
		return e
	}
	return a.writeSlots.Drain(ctx)
}
