// Package otlpfixture is an owned, finite OTLP/HTTP test receiver. It exercises
// the real SDK wire protocol and is not a production tracing backend.
package otlpfixture

import (
	"io"
	"net/http"
	"sync"

	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type Record struct {
	Resource *resource.Resource
	Scope    *common.InstrumentationScope
	Span     *trace.Span
}
type Receiver struct {
	mu       sync.Mutex
	records  []Record
	limit    int
	rejected int
	slots    chan struct{}
}

func New(limit int) *Receiver {
	if limit < 1 || limit > 8192 {
		panic("invalid OTLP fixture bound")
	}
	return &Receiver{limit: limit, slots: make(chan struct{}, 2)}
}
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" || req.URL.Path != "/v1/traces" {
		http.NotFound(w, req)
		return
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	default:
		w.WriteHeader(503)
		return
	}
	if req.Header.Get("Content-Type") != "application/x-protobuf" {
		w.WriteHeader(415)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, 1<<20))
	if err != nil {
		w.WriteHeader(413)
		return
	}
	var batch collector.ExportTraceServiceRequest
	if proto.Unmarshal(body, &batch) != nil {
		w.WriteHeader(400)
		return
	}
	r.mu.Lock()
	for _, resourceSpans := range batch.ResourceSpans {
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				if len(r.records) == r.limit || proto.Size(resourceSpans.Resource)+proto.Size(scopeSpans.Scope)+proto.Size(span) > 8192 {
					r.rejected++
					continue
				}
				saved := Record{Span: proto.Clone(span).(*trace.Span)}
				if resourceSpans.Resource != nil {
					saved.Resource = proto.Clone(resourceSpans.Resource).(*resource.Resource)
				}
				if scopeSpans.Scope != nil {
					saved.Scope = proto.Clone(scopeSpans.Scope).(*common.InstrumentationScope)
				}
				r.records = append(r.records, saved)
			}
		}
	}
	r.mu.Unlock()
	out, _ := proto.Marshal(&collector.ExportTraceServiceResponse{})
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(out)
}
func (r *Receiver) Snapshot() ([]Record, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Record, 0, len(r.records))
	for _, record := range r.records {
		copy := Record{Span: proto.Clone(record.Span).(*trace.Span)}
		if record.Resource != nil {
			copy.Resource = proto.Clone(record.Resource).(*resource.Resource)
		}
		if record.Scope != nil {
			copy.Scope = proto.Clone(record.Scope).(*common.InstrumentationScope)
		}
		out = append(out, copy)
	}
	return out, r.rejected
}
func (record Record) Service() string {
	for _, attribute := range record.Resource.GetAttributes() {
		if attribute.Key == "service.name" {
			return attribute.Value.GetStringValue()
		}
	}
	return ""
}
