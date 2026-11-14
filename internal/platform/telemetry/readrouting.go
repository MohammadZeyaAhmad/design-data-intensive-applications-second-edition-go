package telemetry

import (
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/readrouting"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterReadRouting uses only the router's fixed, validated replica labels.
// Scrapes take memory snapshots; they do not issue database queries or export
// tenant/session identifiers, token bytes, WAL positions or DSNs.
func (r *Runtime) RegisterReadRouting(router *readrouting.Router) {
	if !r.Options.Enabled || router == nil {
		return
	}
	for i := range router.Stats().Probes {
		labels := prometheus.Labels{"replica": router.Stats().Probes[i].Label}
		current := func() readrouting.ProbeStats { return router.Stats().Probes[i] }
		r.Registry.MustRegister(
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "atlas_read_replica_probe_up", Help: "Latest request-driven replica role/history probe succeeded; zero also before first probe.", ConstLabels: labels}, func() float64 {
				if current().OK {
					return 1
				}
				return 0
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "atlas_read_replica_fence_gap_bytes", Help: "Latest request session fence minus replay position, clamped at zero; interpret with probe_up and observation timestamp, not as wall-clock lag.", ConstLabels: labels}, func() float64 { return float64(current().GapBytes) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "atlas_read_replica_probe_timestamp_seconds", Help: "Latest request-driven probe completion time; zero means unobserved.", ConstLabels: labels}, func() float64 { return current().ObservedAt }),
		)
	}
	r.Registry.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "atlas_read_pending_total", Help: "Replica waits ending in explicit pending responses."}, func() float64 { return float64(router.Stats().Pending) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "atlas_read_primary_fallback_total", Help: "Replica waits selecting explicit primary fallback."}, func() float64 { return float64(router.Stats().Fallbacks) }),
	)
}
