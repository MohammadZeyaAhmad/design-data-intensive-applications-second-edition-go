package readrouting

import (
	"sync"
	"time"
)

// ProbeStats describe the latest request-driven probe. GapBytes measures
// distance from THAT request's session fence, not current-primary wall lag.
// Inspect OK and ObservedAt before interpreting it as a fresh observation.
type ProbeStats struct {
	Label      string
	OK         bool
	GapBytes   uint64
	ObservedAt float64
}
type Stats struct {
	Probes             []ProbeStats
	Pending, Fallbacks uint64
}
type probeState struct {
	sync.Mutex
	value ProbeStats
}

func (r *Router) recordProbe(name string, ok bool, required, replayed LSN) {
	s := r.probes[name]
	s.Lock()
	defer s.Unlock()
	gap := uint64(0)
	if ok && required > replayed {
		gap = uint64(required - replayed)
	}
	s.value = ProbeStats{Label: name, OK: ok, GapBytes: gap, ObservedAt: float64(time.Now().UnixNano()) / 1e9}
}
func (r *Router) Stats() Stats {
	out := Stats{Pending: r.pending.Load(), Fallbacks: r.fallbacks.Load(), Probes: make([]ProbeStats, 0, len(r.replicas))}
	for _, n := range r.replicas {
		s := r.probes[n.Label]
		s.Lock()
		p := s.value
		s.Unlock()
		p.Label = n.Label
		out.Probes = append(out.Probes, p)
	}
	return out
}
