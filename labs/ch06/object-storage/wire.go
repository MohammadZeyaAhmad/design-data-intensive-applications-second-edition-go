package objectstorage

import (
	"io"
	"net/http"
	"sync"
)

type WireCounts struct {
	Requests  uint64 `json:"http_requests"`
	Gets      uint64 `json:"get_requests"`
	Puts      uint64 `json:"put_requests"`
	Lists     uint64 `json:"list_requests"`
	Deletes   uint64 `json:"delete_requests"`
	Heads     uint64 `json:"head_requests"`
	Rejected  uint64 `json:"http_precondition_conflicts"`
	ReadBytes uint64 `json:"response_body_bytes"`
	PutBytes  uint64 `json:"attempted_put_body_bytes"`
}
type wireMeter struct {
	next   http.RoundTripper
	mu     sync.Mutex
	counts WireCounts
}

func (w *wireMeter) snapshot() WireCounts { w.mu.Lock(); defer w.mu.Unlock(); return w.counts }
func (w *wireMeter) RoundTrip(r *http.Request) (*http.Response, error) {
	w.mu.Lock()
	w.counts.Requests++
	switch r.Method {
	case "GET":
		if r.URL.Query().Get("list-type") == "2" {
			w.counts.Lists++
		} else {
			w.counts.Gets++
		}
	case "PUT":
		w.counts.Puts++
		if r.ContentLength > 0 {
			w.counts.PutBytes += uint64(r.ContentLength)
		}
	case "DELETE":
		w.counts.Deletes++
	case "HEAD":
		w.counts.Heads++
	}
	w.mu.Unlock()
	response, err := w.next.RoundTrip(r)
	if err != nil {
		return response, err
	}
	w.mu.Lock()
	if response.StatusCode == 409 || response.StatusCode == 412 {
		w.counts.Rejected++
	}
	w.mu.Unlock()
	response.Body = &wireBody{ReadCloser: response.Body, meter: w}
	return response, nil
}

type wireBody struct {
	io.ReadCloser
	meter *wireMeter
}

func (b *wireBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.meter.mu.Lock()
	b.meter.counts.ReadBytes += uint64(n)
	b.meter.mu.Unlock()
	return n, err
}
