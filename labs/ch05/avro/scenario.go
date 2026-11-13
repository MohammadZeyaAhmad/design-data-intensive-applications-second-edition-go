package avrolab

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
)

type Measurement struct {
	Iterations     int    `json:"iterations"`
	WallNanos      int64  `json:"wall_nanos_total"`
	AllocatedBytes uint64 `json:"allocated_bytes_total"`
	Allocations    uint64 `json:"allocations_total"`
}
type Sample struct {
	Version string      `json:"version"`
	Mode    string      `json:"mode"`
	Lines   int         `json:"lines"`
	Run     int         `json:"run"`
	Bytes   int         `json:"encoded_bytes"`
	Digest  string      `json:"fixture_digest"`
	Encode  Measurement `json:"encode"`
	Decode  Measurement `json:"decode"`
}
type Compatibility struct {
	Writer             string `json:"writer"`
	Reader             string `json:"reader"`
	Mode               string `json:"mode"`
	Compatible         bool   `json:"compatible"`
	ExpectedCompatible bool   `json:"expected_compatible"`
	CommonFieldsEqual  bool   `json:"common_fields_equal"`
	ReaderCurrency     string `json:"reader_currency,omitempty"`
}
type Relay struct {
	Writer            string `json:"writer"`
	Reader            string `json:"relay_reader"`
	Mode              string `json:"mode"`
	CommonFieldsEqual bool   `json:"common_fields_equal"`
	MetadataPreserved bool   `json:"metadata_preserved"`
}
type Archived struct {
	Manifest           Manifest `json:"manifest"`
	ReplayReader       string   `json:"replay_reader"`
	ReaderOnlyRegistry bool     `json:"reader_only_registry"`
	CommonDigests      []string `json:"common_digests"`
}
type Report struct {
	Scenario         string          `json:"scenario"`
	GoVersion        string          `json:"go_version"`
	Runtime          string          `json:"avro_runtime"`
	Boundaries       []string        `json:"measurement_boundaries"`
	Schemas          []SchemaInfo    `json:"schemas"`
	Samples          []Sample        `json:"samples"`
	Compatibility    []Compatibility `json:"compatibility"`
	Relays           []Relay         `json:"relays"`
	Evolution        []Evolution     `json:"evolution_rules"`
	Observations     []Observation   `json:"observations"`
	Dynamic          DynamicResult   `json:"dynamic_catalog_schema"`
	ArchiveDirectory string          `json:"archive_directory"`
	Archives         []Archived      `json:"archives"`
}

func recordDigest(record model.Record) string { b, _ := json.Marshal(record); return hashBytes(b) }
func timed(ctx context.Context, iterations int, fn func() error) (Measurement, error) {
	if e := fn(); e != nil {
		return Measurement{}, e
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		if e := ctx.Err(); e != nil {
			return Measurement{}, e
		}
		if e := fn(); e != nil {
			return Measurement{}, e
		}
	}
	elapsed := time.Since(start).Nanoseconds()
	runtime.ReadMemStats(&after)
	return Measurement{iterations, elapsed, after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs}, nil
}

// RunAt retains named immutable OCF datasets under a fresh caller-owned child
// directory, so repeated runs do not replace an earlier archived dataset.
func RunAt(ctx context.Context, parent string, runs, iterations int) (Report, error) {
	var report Report
	if runs < 1 || runs > 5 || iterations < 1 || iterations > 128 {
		return report, avroprofile.ErrBudget
	}
	if e := ctx.Err(); e != nil {
		return report, e
	}
	r, e := BuiltinRegistry()
	if e != nil {
		return report, e
	}
	report = Report{Scenario: "avro-resolution", GoVersion: runtime.Version(), Runtime: "github.com/twmb/avro v1.9.1", Boundaries: []string{
		"Raw samples use actual binary Avro with authored writer/reader schemas and complete domain validation; they exclude OCF publication, compression, network and disk.",
		"Generated mode uses repository-generated plain Go structs; dynamic mode uses maps and runtime schema metadata. Both use the same pinned codec runtime, with its reflection/unsafe specialization, not generated machine-code codecs.",
		"Warm passes and runtime.GC precede samples. Wall/allocation values are totals for iterations and include bounded preflight, writer/reader resolution and domain mapping; they are not process CPU or universal format rankings.",
		"Full-schema digests include defaults and aliases. CRC-64-AVRO and canonical SHA-256 identify Parsing Canonical Form and may name multiple semantic contracts; ambiguous registry lookup fails.",
		"Owned OCF archives retain full writer schemas, random sync markers, codec metadata and content-addressed manifests. Null/deflate replay performs bounded preflight before actual native OCF decoding; sync markers are not checksums.",
		"Dynamic catalog metadata is a bounded captured fixture, without a live SQL ingestion claim. Recursive/logical schemas and other codecs are outside the accepted input profile.",
	}}
	for _, version := range []string{"v1", "v2", "v3"} {
		info, _ := r.Info(version)
		report.Schemas = append(report.Schemas, info)
	}
	for _, lines := range []int{1, 10, 100} {
		for _, version := range []string{"v1", "v2", "v3"} {
			for _, mode := range []string{"generated", "dynamic"} {
				input := fixture(lines, version)
				wire, e := r.Encode(ctx, version, mode, input)
				if e != nil {
					return report, e
				}
				info, _ := r.Info(version)
				got, e := r.Decode(ctx, info.SHA256, version, mode, wire)
				if e != nil {
					return report, e
				}
				if !reflect.DeepEqual(input, got) {
					return report, fmt.Errorf("%s/%s round trip changed metadata", version, mode)
				}
				for run := 1; run <= runs; run++ {
					encode, e := timed(ctx, iterations, func() error { _, e := r.Encode(ctx, version, mode, input); return e })
					if e != nil {
						return report, e
					}
					decode, e := timed(ctx, iterations, func() error { _, e := r.Decode(ctx, info.SHA256, version, mode, wire); return e })
					if e != nil {
						return report, e
					}
					report.Samples = append(report.Samples, Sample{version, mode, lines, run, len(wire), recordDigest(input), encode, decode})
				}
			}
		}
	}
	for _, mode := range []string{"generated", "dynamic"} {
		for _, writer := range []string{"v1", "v2", "v3"} {
			for _, reader := range []string{"v1", "v2", "v3"} {
				input := fixture(10, writer)
				wire, e := r.Encode(ctx, writer, mode, input)
				if e != nil {
					return report, e
				}
				info, _ := r.Info(writer)
				got, readError := r.Decode(ctx, info.SHA256, reader, mode, wire)
				expected := writer != "v3" || reader == "v3"
				common := readError == nil && reflect.DeepEqual(model.Known(got), model.Known(input))
				currency := ""
				if got.Currency != nil {
					currency = *got.Currency
				}
				if (readError == nil) != expected || expected && !common {
					return report, fmt.Errorf("unexpected matrix %s/%s/%s: %v", mode, writer, reader, readError)
				}
				report.Compatibility = append(report.Compatibility, Compatibility{writer, reader, mode, readError == nil, expected, common, currency})
			}
		}
	}
	for _, mode := range []string{"generated", "dynamic"} {
		for _, writer := range []string{"v1", "v2"} {
			reader := "v1"
			if writer == "v1" {
				reader = "v2"
			}
			input := fixture(10, writer)
			wire, e := r.Encode(ctx, writer, mode, input)
			if e != nil {
				return report, e
			}
			info, _ := r.Info(writer)
			projected, e := r.Decode(ctx, info.SHA256, reader, mode, wire)
			if e != nil {
				return report, e
			}
			projected.Name = "relay:" + projected.Name
			wire, e = r.Encode(ctx, reader, mode, projected)
			if e != nil {
				return report, e
			}
			relayInfo, _ := r.Info(reader)
			returned, e := r.Decode(ctx, relayInfo.SHA256, writer, mode, wire)
			if e != nil {
				return report, e
			}
			input.Name = "relay:" + input.Name
			common := reflect.DeepEqual(model.Known(input), model.Known(returned))
			preserved := reflect.DeepEqual(input, returned)
			if !common || preserved {
				return report, fmt.Errorf("relay did not expose expected metadata loss")
			}
			report.Relays = append(report.Relays, Relay{writer, reader, mode, common, preserved})
		}
	}
	report.Evolution, e = EvolutionCases()
	if e != nil {
		return report, e
	}
	report.Observations, e = observations(ctx, r)
	if e != nil {
		return report, e
	}
	report.Dynamic, e = dynamicScenario()
	if e != nil {
		return report, e
	}
	if parent == "" {
		parent = os.TempDir()
	}
	if e = os.MkdirAll(parent, 0700); e != nil {
		return report, e
	}
	dir, e := os.MkdirTemp(parent, "avro-archives-")
	if e != nil {
		return report, e
	}
	published := false
	defer func() {
		if !published {
			os.RemoveAll(dir)
		}
	}()
	report.ArchiveDirectory, e = filepath.Abs(dir)
	if e != nil {
		return report, e
	}
	readerOnly := NewRegistry()
	readerSchema, _ := r.Schema("v3")
	if _, e = readerOnly.Register("v3", readerSchema); e != nil {
		return report, e
	}
	for _, version := range []string{"v1", "v2", "v3"} {
		for _, codec := range []string{"null", "deflate"} {
			input := []model.Record{fixture(1, version), fixture(10, version), fixture(100, version)}
			wire, _, e := r.BuildContainer(ctx, version, "generated", codec, input)
			if e != nil {
				return report, e
			}
			dataset := "orders_" + version + "_" + codec
			manifest, e := PublishDataset(ctx, dir, dataset, wire)
			if e != nil {
				return report, e
			}
			records, replayed, e := readerOnly.ReplayDataset(ctx, dir, dataset, "v3")
			if e != nil || len(records) != len(input) || replayed != manifest {
				return report, fmt.Errorf("immutable dataset replay failed: %v", e)
			}
			archived := Archived{Manifest: manifest, ReplayReader: "v3", ReaderOnlyRegistry: true}
			for i, record := range records {
				if !reflect.DeepEqual(model.Known(record), model.Known(input[i])) {
					return report, fmt.Errorf("archive replay changed common fields")
				}
				archived.CommonDigests = append(archived.CommonDigests, recordDigest(model.Known(record)))
			}
			report.Archives = append(report.Archives, archived)
		}
	}
	published = true
	return report, nil
}
