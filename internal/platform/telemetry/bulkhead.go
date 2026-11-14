package telemetry

import (
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/resilience"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterBulkhead exposes only a fixed workload name and numeric snapshots.
func (r *Runtime) RegisterBulkhead(name string, snapshot func() resilience.BulkheadStats) {
	if !r.Options.Enabled {
		return
	}
	switch name {
	case "http_read", "http_write", "catalog_origin", "catalog_cache", "payment_provider":
	default:
		panic("unbounded bulkhead label")
	}
	labels := prometheus.Labels{"workload": name}
	r.Registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "atlas_bulkhead_active", Help: "Currently admitted physical dependency/HTTP work.", ConstLabels: labels}, func() float64 { return float64(snapshot().Active) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "atlas_bulkhead_queued", Help: "Current bounded FIFO waiting population.", ConstLabels: labels}, func() float64 { return float64(snapshot().Queued) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "atlas_bulkhead_rejected_total", Help: "Rejected finite-resource admissions.", ConstLabels: labels}, func() float64 { return float64(snapshot().Rejected) }),
	)
}
