// Package encoding compares bounded, versioned encodings of synthetic order
// snapshots. These contracts are laboratory fixtures, not live event formats.
package encoding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

type Metrics struct {
	Iterations           int    `json:"iterations"`
	WallNanos            int64  `json:"wall_nanos_total"`
	CPUNanos             int64  `json:"process_cpu_nanos_total"`
	ParentAllocatedBytes uint64 `json:"parent_allocated_bytes_total"`
	ParentAllocations    uint64 `json:"parent_allocations_total"`
}
type Sample struct {
	Format        string  `json:"format"`
	Version       int     `json:"version"`
	Lines         int     `json:"lines"`
	Run           int     `json:"run"`
	EncodedBytes  int     `json:"encoded_bytes"`
	FixtureDigest string  `json:"fixture_digest"`
	Encode        Metrics `json:"encode"`
	Decode        Metrics `json:"decode"`
}
type Compatibility struct {
	Format                string `json:"format"`
	Writer                int    `json:"writer_version"`
	Reader                int    `json:"reader_version"`
	CommonFieldsEqual     bool   `json:"common_fields_equal"`
	WriterFieldsPreserved bool   `json:"writer_fields_preserved"`
}
type Relay struct {
	Format            string `json:"format"`
	Writer            int    `json:"writer_version"`
	Reader            int    `json:"relay_version"`
	Mode              string `json:"mode"`
	CommonFieldsEqual bool   `json:"common_fields_equal"`
	MetadataPreserved bool   `json:"version_metadata_preserved"`
}
type Report struct {
	Scenario      string          `json:"scenario"`
	GoVersion     string          `json:"go_version"`
	Platform      string          `json:"platform"`
	Boundaries    []string        `json:"measurement_boundaries"`
	Samples       []Sample        `json:"samples"`
	Compatibility []Compatibility `json:"compatibility"`
	Relays        []Relay         `json:"relays"`
	RPC           []Relay         `json:"generated_thrift_rpc"`
	Probes        []Probe         `json:"observations"`
}

func digest(r model.Record) string {
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func measure(ctx context.Context, iterations int, fn func() error) (Metrics, error) {
	// Warm compilation/schema caches and correctness checks happen outside timing.
	if e := fn(); e != nil {
		return Metrics{}, e
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpu, e := processCPU()
	if e != nil {
		return Metrics{}, e
	}
	start := time.Now()
	for i := 0; i < iterations; i++ {
		if e = ctx.Err(); e != nil {
			return Metrics{}, e
		}
		if e = fn(); e != nil {
			return Metrics{}, e
		}
	}
	wall := time.Since(start).Nanoseconds()
	endCPU, e := processCPU()
	if e != nil {
		return Metrics{}, e
	}
	runtime.ReadMemStats(&after)
	return Metrics{iterations, wall, endCPU - cpu, after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs}, nil
}

// Run records raw repeated samples. CPU is process user+system time (including
// waited-for gob children), not hardware cycles or a universal format ranking.
func Run(ctx context.Context, runs, iterations int) (Report, error) {
	var report Report
	if runs < 1 || runs > 5 || iterations < 1 || iterations > 128 {
		return report, model.ErrBudget
	}
	s, e := NewSuite(ctx)
	if e != nil {
		return report, e
	}
	defer s.Close()
	report, e = s.run(ctx, runs, iterations)
	return report, e
}
func (s *Suite) run(ctx context.Context, runs, iterations int) (Report, error) {
	report := Report{Scenario: "encoding-compatibility", GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
		Boundaries: []string{
			"Fresh self-contained record per encode; no compression, encryption, persistence, network or stream type-table amortization.",
			"All timed paths include application validation and bounded preflight; JSON includes JSON Schema validation, the others use native schema/type checks plus domain validation.",
			"Metrics are totals for iterations; process CPU includes parent and reaped child user+system time. Background GC and scheduling affect measurements.",
			"Gob decode includes a fresh isolated process, IPC and JSON return per record; helper compilation is setup and excluded. Other decoders run in process.",
			"Allocated bytes/objects describe the parent Go process only; gob child heap and all OS/kernel memory are excluded.",
			"A warm pass precedes each sample; independent raw repetitions and complete fixture digests are retained. Values establish no general speed ranking.",
		}}
	for _, lines := range []int{1, 10, 100} {
		for _, format := range Formats {
			for version := 1; version <= 2; version++ {
				r := model.Fixture(lines, version)
				wire, e := s.Encode(format, version, r)
				if e != nil {
					return report, e
				}
				got, e := s.Decode(ctx, format, version, wire)
				if e != nil || !reflect.DeepEqual(r, got) {
					return report, fmt.Errorf("%s v%d round trip differs: %w", format, version, e)
				}
				for run := 1; run <= runs; run++ {
					encode, e := measure(ctx, iterations, func() error { _, err := s.Encode(format, version, r); return err })
					if e != nil {
						return report, e
					}
					decode, e := measure(ctx, iterations, func() error { _, err := s.Decode(ctx, format, version, wire); return err })
					if e != nil {
						return report, e
					}
					report.Samples = append(report.Samples, Sample{format, version, lines, run, len(wire), digest(r), encode, decode})
				}
			}
		}
	}
	for _, format := range Formats {
		for writer := 1; writer <= 2; writer++ {
			r := model.Fixture(10, writer)
			wire, e := s.Encode(format, writer, r)
			if e != nil {
				return report, e
			}
			for reader := 1; reader <= 2; reader++ {
				got, e := s.Decode(ctx, format, reader, wire)
				if e != nil {
					return report, e
				}
				common := reflect.DeepEqual(model.Known(got), model.Known(r))
				if !common {
					return report, fmt.Errorf("%s compatibility %d/%d changed common fields", format, writer, reader)
				}
				report.Compatibility = append(report.Compatibility, Compatibility{format, writer, reader, common, reflect.DeepEqual(got, r)})
				if reader == writer {
					continue
				}
				got.Name = "relay:" + got.Name
				projected, e := s.Encode(format, reader, got)
				if e != nil {
					return report, e
				}
				returned, e := s.Decode(ctx, format, writer, projected)
				if e != nil {
					return report, e
				}
				r.Name = "relay:" + r.Name
				common = reflect.DeepEqual(model.Known(returned), model.Known(r))
				if !common || reflect.DeepEqual(returned, r) {
					return report, fmt.Errorf("%s domain relay did not expose expected metadata loss", format)
				}
				report.Relays = append(report.Relays, Relay{format, writer, reader, "domain-projection", common, false})
				// Preserve the original input for the next matrix iteration.
				r.Name = model.Fixture(10, writer).Name
			}
			if format == "protobuf" {
				reader := 3 - writer
				preserved, e := PreserveProtoRelay(reader, wire, "relay:"+r.Name)
				if e != nil {
					return report, e
				}
				got, e := s.Decode(ctx, format, writer, preserved)
				r.Name = "relay:" + r.Name
				if e != nil || !reflect.DeepEqual(got, r) {
					return report, fmt.Errorf("native generated Protobuf relay lost unknown fields: %w", e)
				}
				report.Relays = append(report.Relays, Relay{format, writer, reader, "native-generated-message", true, true})
			}
		}
	}
	r := model.Fixture(10, 2)
	wire, _ := s.Encode("json", 2, r)
	wire, e := PreserveJSONRelay(wire, "relay:"+r.Name)
	if e != nil {
		return report, e
	}
	r.Name = "relay:" + r.Name
	got, e := s.Decode(ctx, "json", 2, wire)
	if e != nil || !reflect.DeepEqual(got, r) {
		return report, fmt.Errorf("raw JSON relay lost metadata: %w", e)
	}
	report.Relays = append(report.Relays, Relay{"json", 2, 1, "raw-object-preserving-known-field-edit", true, true})
	for _, format := range []string{"thrift-binary", "thrift-compact"} {
		for client := 1; client <= 2; client++ {
			for server := 1; server <= 2; server++ {
				r := model.Fixture(10, client)
				got, e := GeneratedRPC(ctx, format, client, server, r)
				r.Name = "relay:" + r.Name
				common := reflect.DeepEqual(model.Known(got), model.Known(r))
				preserved := reflect.DeepEqual(got, r)
				if e != nil || !common || preserved != (client == server) {
					return report, fmt.Errorf("generated %s RPC %d/%d differs: %w", format, client, server, e)
				}
				report.RPC = append(report.RPC, Relay{format, client, server, "generated-client-processor", common, preserved})
			}
		}
	}
	report.Probes, e = s.Probes(ctx)
	return report, e
}
