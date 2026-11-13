package avrolab

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

func registry(t testing.TB) *Registry {
	t.Helper()
	r, e := BuiltinRegistry()
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestWriterReaderMatrixGeneratedAndDynamic(t *testing.T) {
	r := registry(t)
	ctx := context.Background()
	for _, mode := range []string{"generated", "dynamic"} {
		for _, writer := range []string{"v1", "v2", "v3"} {
			for _, reader := range []string{"v1", "v2", "v3"} {
				t.Run(mode+writer+reader, func(t *testing.T) {
					in := fixture(10, writer)
					b, e := r.Encode(ctx, writer, mode, in)
					if e != nil {
						t.Fatal(e)
					}
					id, _ := r.Info(writer)
					out, e := r.Decode(ctx, id.SHA256, reader, mode, b)
					if writer == "v3" && reader != "v3" {
						if !errors.Is(e, ErrIncompatible) || !reflect.DeepEqual(out, model.Record{}) {
							t.Fatal("incompatible pair accepted", e)
						}
						return
					}
					if e != nil {
						t.Fatal(e)
					}
					if !reflect.DeepEqual(model.Known(out), model.Known(in)) {
						t.Fatal("common fields changed", out, in)
					}
					if writer == reader && !reflect.DeepEqual(out, in) {
						t.Fatal("same-version round trip changed metadata", out, in)
					}
					if writer == "v1" && reader != "v1" {
						currency := "USD"
						if reader == "v3" {
							currency = "EUR"
						}
						if out.Currency == nil || *out.Currency != currency {
							t.Fatal("reader default did not apply")
						}
						if reader == "v3" && (out.Discount == nil || *out.Discount != 0) {
							t.Fatal("union reader default did not apply")
						}
					}
				})
			}
		}
	}
}
func TestSingleObjectExactLookupMissingAndAmbiguous(t *testing.T) {
	r := registry(t)
	ctx := context.Background()
	in := fixture(3, "v2")
	wire, e := r.EncodeSingleObject(ctx, "v2", "generated", in)
	if e != nil {
		t.Fatal(e)
	}
	fp, payload, e := avro.SingleObjectFingerprint(wire)
	id, _ := r.Info("v2")
	writer, _ := r.byVersion("v2")
	if e != nil || binary.LittleEndian.Uint64(fp[:]) != writer.fingerprint || len(payload)+10 != len(wire) {
		t.Fatal("nonstandard single-object header")
	}
	got, e := r.DecodeSingleObject(ctx, "v2", "dynamic", wire)
	if e != nil || !reflect.DeepEqual(got, in) {
		t.Fatal(got, e)
	}
	missing := NewRegistry()
	raw, _ := r.Schema("v3")
	missing.Register("v3", raw)
	if _, e = missing.DecodeSingleObject(ctx, "v3", "dynamic", wire); !errors.Is(e, ErrMissingSchema) {
		t.Fatal("latest reader guessed missing writer", e)
	}
	if _, e = missing.Decode(ctx, id.SHA256, "v3", "generated", payload); !errors.Is(e, ErrMissingSchema) {
		t.Fatal(e)
	}
	raw, _ = r.Schema("v2")
	changed := bytes.Replace(raw, []byte(`"default":"USD"`), []byte(`"default":"EUR"`), 1)
	if bytes.Equal(raw, changed) {
		t.Fatal("fixture mutation missed")
	}
	info, e := r.Register("v2_eur", changed)
	if e != nil {
		t.Fatal(e)
	}
	if info.Rabin != id.Rabin || info.CanonicalSHA256 != id.CanonicalSHA256 || info.SHA256 == id.SHA256 {
		t.Fatal("full identity confused with parsing canonical form")
	}
	if _, e = r.DecodeSingleObject(ctx, "v2", "dynamic", wire); !errors.Is(e, ErrAmbiguous) {
		t.Fatal("ambiguous full contract silently selected", e)
	}
	got, e = r.Decode(ctx, id.SHA256, "v2", "generated", payload)
	if e != nil || !reflect.DeepEqual(got, in) {
		t.Fatal("explicit immutable identity stopped working", e)
	}
	got, e = r.DecodePinnedSingleObject(ctx, id.SHA256, "v2", "dynamic", wire)
	if e != nil || !reflect.DeepEqual(got, in) {
		t.Fatal("pinned single-object identity failed", e)
	}
	oldID, _ := r.Info("v1")
	if _, e = r.DecodePinnedSingleObject(ctx, oldID.SHA256, "v2", "generated", wire); e == nil {
		t.Fatal("pinned lookup ignored mismatched header")
	}
}
func TestRegistryImmutableCopiesAndConcurrentReads(t *testing.T) {
	r := registry(t)
	raw, _ := r.Schema("v1")
	changed := bytes.Replace(raw, []byte("immutable"), []byte("mutable"), 1)
	if _, e := r.Register("v1", changed); !errors.Is(e, ErrImmutable) {
		t.Fatal("version overwritten", e)
	}
	raw[0] = '!'
	again, _ := r.Schema("v1")
	if again[0] != '{' {
		t.Fatal("caller mutated registry")
	}
	ctx := context.Background()
	id, _ := r.Info("v2")
	var group sync.WaitGroup
	for i := 0; i < 12; i++ {
		group.Go(func() {
			for j := 0; j < 12; j++ {
				input := fixture(10, "v2")
				wire, e := r.Encode(ctx, "v2", "generated", input)
				if e != nil {
					t.Error(e)
					return
				}
				got, e := r.Decode(ctx, id.SHA256, "v3", "dynamic", wire)
				if e != nil || !reflect.DeepEqual(model.Known(got), model.Known(input)) {
					t.Error("concurrent schema operation changed data", e)
					return
				}
			}
		})
	}
	group.Wait()
}
func TestContainerReplayCarriesActualWriterSchemaAndDefaults(t *testing.T) {
	r := registry(t)
	ctx := context.Background()
	onlyReader := NewRegistry()
	raw, _ := r.Schema("v3")
	onlyReader.Register("v3", raw)
	for _, codec := range []string{"null", "deflate"} {
		for _, mode := range []string{"generated", "dynamic"} {
			input := []model.Record{fixture(1, "v1"), fixture(10, "v1"), fixture(100, "v1")}
			wire, info, e := r.BuildContainer(ctx, "v1", mode, codec, input)
			if e != nil {
				t.Fatal(codec, mode, e)
			}
			got, replayed, e := onlyReader.ReplayContainer(ctx, "v3", wire)
			if e != nil || replayed != info || len(got) != 3 {
				t.Fatal("archive needed missing external writer", e)
			}
			for i, record := range got {
				if !reflect.DeepEqual(model.Known(record), model.Known(input[i])) || record.Currency == nil || *record.Currency != "EUR" {
					t.Fatal("archive used latest writer order or wrong default")
				}
			}
			// A bare current reader cannot reinterpret historical writer field order.
			writer, _ := r.byVersion("v1")
			payload, _ := r.Encode(ctx, "v1", mode, input[0])
			latest, _ := r.byVersion("v3")
			var wrong any
			rest, wrongError := latest.schema.Decode(payload, &wrong)
			if wrongError == nil && len(rest) == 0 {
				if equal, e := recordFromMap("v3", wrong); e == nil && reflect.DeepEqual(model.Known(equal), model.Known(input[0])) {
					t.Fatal("fixture failed to expose actual writer necessity")
				}
			}
			if writer.info.SHA256 != info.Writer.SHA256 {
				t.Fatal("archive did not retain exact schema")
			}
		}
	}
}
func TestDatasetPublicationIsImmutableAndChecksWholeFile(t *testing.T) {
	r := registry(t)
	ctx := context.Background()
	root := t.TempDir()
	wire, _, e := r.BuildContainer(ctx, "v1", "generated", "deflate", []model.Record{fixture(10, "v1")})
	if e != nil {
		t.Fatal(e)
	}
	first, e := PublishDataset(ctx, root, "orders_legacy", wire)
	if e != nil {
		t.Fatal(e)
	}
	retry, e := PublishDataset(ctx, root, "orders_legacy", wire)
	if e != nil || retry != first {
		t.Fatal("identical retry failed", e)
	}
	changed, _, e := r.BuildContainer(ctx, "v2", "generated", "null", []model.Record{fixture(10, "v2")})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = PublishDataset(ctx, root, "orders_legacy", changed); !errors.Is(e, ErrImmutable) {
		t.Fatal("dataset replaced", e)
	}
	fresh := NewRegistry()
	raw, _ := r.Schema("v3")
	fresh.Register("v3", raw)
	got, manifest, e := fresh.ReplayDataset(ctx, root, "orders_legacy", "v3")
	if e != nil || manifest != first || len(got) != 1 || !reflect.DeepEqual(model.Known(got[0]), model.Known(fixture(10, "v1"))) {
		t.Fatal("fresh reader failed immutable replay", e)
	}
	path := filepath.Join(root, "objects", first.FileSHA256+".avro")
	bad := append([]byte(nil), wire...)
	bad[len(bad)-1] ^= 1
	if e = os.WriteFile(path, bad, 0600); e != nil {
		t.Fatal(e)
	}
	if records, _, e := fresh.ReplayDataset(ctx, root, "orders_legacy", "v3"); e == nil || records != nil {
		t.Fatal("corrupt archive returned partial success")
	}
}
func TestBoundedMalformedAvroAndOCF(t *testing.T) {
	r := registry(t)
	ctx := context.Background()
	id, _ := r.Info("v2")
	wire, _ := r.Encode(ctx, "v2", "generated", fixture(3, "v2"))
	for _, bad := range [][]byte{nil, wire[:1], wire[:len(wire)-1], append(append([]byte{}, wire...), 0xff), bytes.Repeat([]byte{0}, avroprofile.MaxWire+1)} {
		if got, e := r.Decode(ctx, id.SHA256, "v2", "generated", bad); e == nil || !reflect.DeepEqual(got, model.Record{}) {
			t.Fatal("bad record returned partial success")
		}
	}
	archive, _, e := r.BuildContainer(ctx, "v2", "generated", "deflate", []model.Record{fixture(3, "v2")})
	if e != nil {
		t.Fatal(e)
	}
	for _, bad := range [][]byte{archive[:3], archive[:len(archive)-1], append(append([]byte{}, archive...), 0), bytes.Repeat([]byte{0}, MaxArchive+1)} {
		if got, _, e := r.ReplayContainer(ctx, "v2", bad); e == nil || got != nil {
			t.Fatal("bad container accepted")
		}
	}
	for _, schema := range []string{`{"type":"record","name":"R","fields":[{"name":"self","type":"R"}]}`, `{"type":"fixed","name":"F","size":2147483647}`, `{"type":"long","logicalType":"timestamp-millis"}`, `{"type":"int","type":"long"}`} {
		if _, _, _, e = avroprofile.Compile([]byte(schema)); e == nil {
			t.Fatal("unbounded/out-of-profile schema accepted")
		}
	}
	// Huge zero-byte array counts are rejected before a native allocation/loop.
	_, plan, _, e := avroprofile.Compile([]byte(`{"type":"array","items":"null"}`))
	if e != nil {
		t.Fatal(e)
	}
	var large [10]byte
	n := binary.PutUvarint(large[:], uint64(1<<32))
	if e = avroprofile.Scan(plan, append(large[:n], 0)); e == nil {
		t.Fatal("huge array accepted")
	}
}
func FuzzBoundedAvro(f *testing.F) {
	r := registry(f)
	for _, version := range []string{"v1", "v2", "v3"} {
		wire, e := r.Encode(context.Background(), version, "generated", fixture(2, version))
		if e != nil {
			f.Fatal(e)
		}
		f.Add(version, wire)
	}
	f.Fuzz(func(t *testing.T, version string, wire []byte) {
		if len(wire) > avroprofile.MaxWire {
			return
		}
		id, e := r.Info(version)
		if e != nil {
			return
		}
		record, e := r.Decode(context.Background(), id.SHA256, version, "dynamic", wire)
		if e == nil && model.Validate(record) != nil {
			t.Fatal("invalid success")
		}
	})
}
func FuzzBoundedContainer(f *testing.F) {
	r := registry(f)
	for _, codec := range []string{"null", "deflate"} {
		wire, _, e := r.BuildContainer(context.Background(), "v1", "generated", codec, []model.Record{fixture(2, "v1")})
		if e != nil {
			f.Fatal(e)
		}
		f.Add(wire)
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > MaxArchive {
			return
		}
		records, _, e := r.ReplayContainer(context.Background(), "v3", wire)
		if e != nil && records != nil {
			t.Fatal("partial success")
		}
		for _, record := range records {
			if model.Validate(record) != nil {
				t.Fatal("invalid record")
			}
		}
	})
}

func TestPresenceAndReaderDefaultIsNotWrittenNull(t *testing.T) {
	r := registry(t)
	ctx := context.Background()
	for _, mode := range []string{"generated", "dynamic"} {
		input := fixture(1, "v2")
		input.Expedited = nil
		input.Discount = nil
		wire, e := r.Encode(ctx, "v2", mode, input)
		if e != nil {
			t.Fatal(e)
		}
		id, _ := r.Info("v2")
		got, e := r.Decode(ctx, id.SHA256, "v3", mode, wire)
		if e != nil || got.Expedited != nil || got.Discount != nil {
			t.Fatal("reader defaults replaced written null", e)
		}
		input = fixture(1, "v2")
		wire, e = r.Encode(ctx, "v2", mode, input)
		if e != nil {
			t.Fatal(e)
		}
		got, e = r.Decode(ctx, id.SHA256, "v3", mode, wire)
		if e != nil || got.Expedited == nil || *got.Expedited || got.Discount == nil || *got.Discount != 0 {
			t.Fatal("false/zero became null", e)
		}
	}
}

func TestManifestJSONIsOrdinaryReviewableMetadata(t *testing.T) {
	r := registry(t)
	wire, _, e := r.BuildContainer(context.Background(), "v1", "dynamic", "null", []model.Record{fixture(1, "v1")})
	if e != nil {
		t.Fatal(e)
	}
	root := t.TempDir()
	manifest, e := PublishDataset(context.Background(), root, "legacy", wire)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(root, "datasets", "legacy.json"))
	if e != nil {
		t.Fatal(e)
	}
	var decoded Manifest
	if json.Unmarshal(raw, &decoded) != nil || decoded != manifest {
		t.Fatal("manifest not round trippable")
	}
}

func TestEvolutionRulesAndDefaultIdentityObservations(t *testing.T) {
	r := registry(t)
	cases, e := EvolutionCases()
	if e != nil {
		t.Fatal(e)
	}
	if len(cases) != 26 {
		t.Fatal("missing evolution rule")
	}
	for _, c := range cases {
		if c.Compatible != c.ExpectedCompatible {
			t.Fatal(c)
		}
	}
	observations, e := observations(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	if len(observations) != 9 {
		t.Fatal("missing schema identity/default observation")
	}
	for _, observation := range observations {
		if !observation.Verified {
			t.Fatal(observation.ID)
		}
	}
}
func TestDynamicCatalogSchemasAreIndependentOfMapOrInputOrder(t *testing.T) {
	result, e := dynamicScenario()
	if e != nil {
		t.Fatal(e)
	}
	if !result.StableMetadataOrder || !result.NewNullableDefault || !result.ReverseRejected || result.PromotionValue != 7 {
		t.Fatal(result)
	}
	for _, columns := range [][]Column{{{"x", "text", false, 1}, {"x", "integer", true, 2}}, {{"x", "text", false, 1}, {"y", "integer", true, 1}}, {{"x", "numeric", false, 1}}, {{"bad-name", "text", false, 1}}} {
		if _, e = DynamicSchema(columns); e == nil {
			t.Fatal("ambiguous/unsupported column metadata accepted")
		}
	}
}
func TestReportChecksAllSchemasModesAndRetainsActualArchives(t *testing.T) {
	report, e := RunAt(context.Background(), t.TempDir(), 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if len(report.Samples) != 18 || len(report.Compatibility) != 18 || len(report.Relays) != 4 || len(report.Archives) != 6 || len(report.Evolution) != 26 || len(report.Observations) != 9 {
		t.Fatal("incomplete report")
	}
	for _, sample := range report.Samples {
		if sample.Digest != recordDigest(fixture(sample.Lines, sample.Version)) || sample.Bytes < 1 || sample.Encode.WallNanos < 1 || sample.Decode.WallNanos < 1 {
			t.Fatal("invalid raw sample")
		}
	}
	for _, archive := range report.Archives {
		path := filepath.Join(report.ArchiveDirectory, "objects", archive.Manifest.FileSHA256+".avro")
		if _, e = os.Stat(path); e != nil {
			t.Fatal("report points to missing archive", e)
		}
	}
}

func TestHostileContainerCountsAndDecompressionAreBounded(t *testing.T) {
	r := registry(t)
	writer, _ := r.byVersion("v1")
	ctx := context.Background()
	appendLong := func(dst []byte, n int64) []byte { return binary.AppendUvarint(dst, uint64(n)<<1^uint64(n>>63)) }
	appendBytes := func(dst, b []byte) []byte { return append(appendLong(dst, int64(len(b))), b...) }
	header := []byte{'O', 'b', 'j', 1}
	header = appendLong(header, 4)
	for _, field := range [][2]string{{"avro.schema", string(writer.full)}, {"avro.codec", "deflate"}, {"atlas.schema.version", "v1"}, {"atlas.schema.sha256", writer.info.SHA256}} {
		header = appendBytes(header, []byte(field[0]))
		header = appendBytes(header, []byte(field[1]))
	}
	header = appendLong(header, 0)
	marker := bytes.Repeat([]byte{0x42}, 16)
	header = append(header, marker...)
	var compressed bytes.Buffer
	z, e := flate.NewWriter(&compressed, flate.BestCompression)
	if e != nil {
		t.Fatal(e)
	}
	z.Write(bytes.Repeat([]byte{0}, avroprofile.MaxWire+1))
	z.Close()
	bomb := appendLong(append([]byte{}, header...), 1)
	bomb = appendBytes(bomb, compressed.Bytes())
	bomb = append(bomb, marker...)
	hugeCount := appendLong(append([]byte{}, header...), 1<<40)
	hugeCount = appendLong(hugeCount, 0)
	hugeCount = append(hugeCount, marker...)
	hugeBlock := appendLong(append([]byte{}, header...), 1)
	hugeBlock = appendLong(hugeBlock, 1<<40)
	for _, wire := range [][]byte{bomb, hugeCount, hugeBlock} {
		if records, _, e := r.ReplayContainer(ctx, "v3", wire); e == nil || records != nil {
			t.Fatal("hostile archive escaped bounds")
		}
	}
}

func TestConcurrentDatasetPublicationHasOneCompleteWinner(t *testing.T) {
	r := registry(t)
	ctx := context.Background()
	root := t.TempDir()
	var group sync.WaitGroup
	outcomes := make(chan Manifest, 12)
	for i := 0; i < 12; i++ {
		group.Go(func() {
			record := fixture(3, "v1")
			record.Name = fmt.Sprintf("writer-%02d", i)
			wire, _, e := r.BuildContainer(ctx, "v1", "generated", "null", []model.Record{record})
			if e != nil {
				t.Error(e)
				return
			}
			manifest, e := PublishDataset(ctx, root, "contended", wire)
			if e == nil {
				outcomes <- manifest
			} else if !errors.Is(e, ErrImmutable) {
				t.Error(e)
			}
		})
	}
	group.Wait()
	close(outcomes)
	winners := []Manifest{}
	for manifest := range outcomes {
		winners = append(winners, manifest)
	}
	if len(winners) != 1 {
		t.Fatal("expected one immutable publication", len(winners))
	}
	records, manifest, e := r.ReplayDataset(ctx, root, "contended", "v3")
	if e != nil || manifest != winners[0] || len(records) != 1 {
		t.Fatal("published a partial or mismatched winner", e)
	}
}
