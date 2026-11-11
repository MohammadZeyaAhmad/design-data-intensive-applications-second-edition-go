package encoding

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

var testSuite *Suite

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, e := NewSuite(ctx)
	if e != nil {
		panic(e)
	}
	testSuite = s
	code := m.Run()
	s.Close()
	os.Exit(code)
}

func TestCompatibilityMatrixAndReadModifyWrite(t *testing.T) {
	for _, format := range Formats {
		for _, writer := range []int{1, 2} {
			for _, reader := range []int{1, 2} {
				t.Run(format+string(rune('0'+writer))+string(rune('0'+reader)), func(t *testing.T) {
					r := model.Fixture(10, writer)
					wire, e := testSuite.Encode(format, writer, r)
					if e != nil {
						t.Fatal(e)
					}
					decoded, e := testSuite.Decode(context.Background(), format, reader, wire)
					if e != nil {
						t.Fatal(e)
					}
					if !reflect.DeepEqual(model.Known(decoded), model.Known(r)) {
						t.Fatal("known fields differ", decoded)
					}
					if writer == reader && !reflect.DeepEqual(decoded, r) {
						t.Fatal("same-version roundtrip differs", decoded, r)
					}
					decoded.Name = "relay:" + decoded.Name
					relay, e := testSuite.Encode(format, reader, decoded)
					if e != nil {
						t.Fatal(e)
					}
					out, e := testSuite.Decode(context.Background(), format, writer, relay)
					if e != nil {
						t.Fatal(e)
					}
					want := r
					want.Name = "relay:" + want.Name
					if !reflect.DeepEqual(model.Known(out), model.Known(want)) {
						t.Fatal("known relay fields differ")
					}
				})
			}
		}
	}
}

func TestGeneratedRelaysAndUnknownFieldRetention(t *testing.T) {
	ctx := context.Background()
	r := model.Fixture(3, 2)
	for _, format := range []string{"thrift-binary", "thrift-compact"} {
		for _, client := range []int{1, 2} {
			for _, server := range []int{1, 2} {
				input := model.Fixture(3, client)
				got, e := GeneratedRPC(ctx, format, client, server, input)
				if e != nil {
					t.Fatal(format, client, server, e)
				}
				want := input
				want.Name = "relay:" + want.Name
				if !reflect.DeepEqual(model.Known(got), model.Known(want)) {
					t.Fatal("generated RPC changed known fields")
				}
				if client == 2 && server == 1 && (got.Currency != nil || got.Discount != nil || len(got.Labels) > 0) {
					t.Fatal("old Thrift relay unexpectedly retained unknown fields")
				}
			}
		}
	}
	wire, _ := testSuite.Encode("protobuf", 2, r)
	relayed, e := PreserveProtoRelay(1, wire, "relay")
	if e != nil {
		t.Fatal(e)
	}
	got, e := testSuite.Decode(ctx, "protobuf", 2, relayed)
	if e != nil || got.Currency == nil || got.Discount == nil || len(got.Labels) != 2 {
		t.Fatal("generated Protobuf relay lost unknown fields", e)
	}
	projected, _ := testSuite.Decode(ctx, "protobuf", 1, wire)
	projected.Name = "relay"
	lost, _ := testSuite.Encode("protobuf", 1, projected)
	got, e = testSuite.Decode(ctx, "protobuf", 2, lost)
	if e != nil || got.Currency != nil || got.Discount != nil || len(got.Labels) != 0 {
		t.Fatal("domain projection did not expose metadata loss")
	}
	jsonWire, _ := testSuite.Encode("json", 2, r)
	jsonRelay, e := PreserveJSONRelay(jsonWire, "relay")
	if e != nil {
		t.Fatal(e)
	}
	got, e = testSuite.Decode(ctx, "json", 2, jsonRelay)
	if e != nil || got.Discount == nil || got.Currency == nil || len(got.Labels) != 2 {
		t.Fatal("raw JSON relay lost fields", e)
	}
}

func TestPresenceJSONSchemaAndNumericLoss(t *testing.T) {
	for _, format := range Formats {
		for _, present := range []bool{false, true} {
			r := model.Fixture(1, 2)
			if !present {
				r.Expedited = nil
				r.Discount = nil
			}
			b, e := testSuite.Encode(format, 2, r)
			if e != nil {
				t.Fatal(e)
			}
			out, e := testSuite.Decode(context.Background(), format, 2, b)
			if e != nil || (out.Expedited != nil) != present || (out.Discount != nil) != present {
				t.Fatal(format, "presence lost", e)
			}
		}
	}
	type pointer struct{ Value *bool }
	f := false
	var b bytes.Buffer
	if e := gob.NewEncoder(&b).Encode(pointer{&f}); e != nil {
		t.Fatal(e)
	}
	var p pointer
	if e := gob.NewDecoder(&b).Decode(&p); e != nil || p.Value != nil {
		t.Fatal("native gob pointer-zero observation changed", e)
	}
	loose, exact, e := NumericJSON()
	if e != nil || exact != "9007199254740993" || loose != 9007199254740992 {
		t.Fatal(loose, exact, e)
	}
	wire, _ := testSuite.Encode("json", 2, model.Fixture(1, 2))
	if e = ValidateJSON(2, wire); e != nil {
		t.Fatal(e)
	}
	var fields map[string]json.RawMessage
	json.Unmarshal(wire, &fields)
	fields["version"] = json.RawMessage(`9007199254740993`)
	invalid, _ := json.Marshal(fields)
	if ValidateJSON(2, invalid) == nil {
		t.Fatal("numeric JSON version bypassed schema")
	}
	if _, e = testSuite.Decode(context.Background(), "json", 2, bytes.Replace(wire, []byte(`"tenant_id":"acme"`), []byte(`"tenant_id":"acme","tenant_id":"globex"`), 1)); e == nil {
		t.Fatal("duplicate JSON key accepted")
	}
}

func TestBoundedMalformedInputs(t *testing.T) {
	ctx := context.Background()
	for _, format := range Formats {
		r := model.Fixture(2, 2)
		wire, e := testSuite.Encode(format, 2, r)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = testSuite.Decode(ctx, format, 2, bytes.Repeat([]byte{0}, model.MaxWire+1)); e == nil {
			t.Fatal(format, "oversized input accepted")
		}
		for _, n := range []int{0, 1, len(wire) / 2, len(wire) - 1} {
			if _, e = testSuite.Decode(ctx, format, 2, wire[:n]); e == nil {
				t.Fatal(format, "truncation accepted", n)
			}
		}
		if _, e = testSuite.Decode(ctx, format, 2, append(append([]byte{}, wire...), 0xff)); e == nil {
			t.Fatal(format, "trailing bytes accepted")
		}
	}
	// Tiny frames with huge declared counts must fail preflight, before allocation.
	if scanMessagePack([]byte{0xdd, 0x7f, 0xff, 0xff, 0xff}) == nil {
		t.Fatal("oversized MessagePack array accepted")
	}
	huge := []byte{15, 0, 4, 12, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(huge[4:], 0x7fffffff)
	if _, e := scanThrift(ctx, "thrift-binary", huge, false); e == nil {
		t.Fatal("oversized Thrift list accepted")
	}
	for _, wire := range []string{`<!DOCTYPE snapshot SYSTEM "file:///etc/passwd"><snapshot/>`, `<snapshot><tenant_id>a</tenant_id><tenant_id>b</tenant_id></snapshot>`} {
		if scanXML([]byte(wire)) == nil {
			t.Fatal("unsafe XML accepted")
		}
	}
	// A constrained worker converts decoder failures into an error, not parent OOM.
	if _, e := testSuite.Decode(ctx, "gob", 2, []byte{0xfe, 0xff, 0xff}); e == nil {
		t.Fatal("invalid gob accepted")
	}
}

func FuzzWirePreflight(f *testing.F) {
	for _, format := range Formats {
		if format == "gob" {
			continue
		}
		wire, _ := testSuite.Encode(format, 2, model.Fixture(2, 2))
		f.Add(format, wire)
	}
	f.Fuzz(func(t *testing.T, format string, b []byte) {
		if len(b) > model.MaxWire || format == "gob" {
			return
		}
		known := false
		for _, v := range Formats {
			known = known || v == format
		}
		if !known {
			return
		}
		r, e := testSuite.Decode(context.Background(), format, 2, b)
		if e == nil && model.Validate(r) != nil {
			t.Fatal("invalid partial success")
		}
	})
}

func TestXMLAndMessagePackDoNotConfuseNumericWidth(t *testing.T) {
	r := model.Fixture(1, 2)
	wire, _ := testSuite.Encode("xml", 2, r)
	wire = bytes.Replace(wire, []byte("<quantity>1</quantity>"), []byte("<quantity>4294967297</quantity>"), 1)
	if _, e := testSuite.Decode(context.Background(), "xml", 1, wire); e == nil {
		t.Fatal("XML narrowed integer")
	}
	raw := to2(r)
	raw.Lines[0].Quantity = 4294967297
	b, e := textEncode("messagepack", 2, raw.record())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testSuite.Decode(context.Background(), "messagepack", 1, b); e == nil {
		t.Fatal("MessagePack narrowed integer")
	}
	if ValidateJSON(2, []byte(strings.Repeat(" ", model.MaxWire+1))) == nil {
		t.Fatal("unbounded schema input")
	}
}

func TestReportRetainsIndependentFixturesAndSemanticObservations(t *testing.T) {
	report, e := testSuite.run(context.Background(), 1, 1)
	if e != nil {
		t.Fatal(e)
	}
	if len(report.Samples) != 42 || len(report.Compatibility) != 28 || len(report.Relays) != 17 || len(report.RPC) != 8 || len(report.Probes) != 12 {
		t.Fatal("report silently omitted a format, version, direction or observation")
	}
	for _, sample := range report.Samples {
		if sample.FixtureDigest != digest(model.Fixture(sample.Lines, sample.Version)) || sample.EncodedBytes < 1 || sample.Encode.WallNanos < 1 || sample.Decode.CPUNanos < 0 {
			t.Fatal("invalid sample boundary", sample)
		}
	}
	for _, c := range report.Compatibility {
		if !c.CommonFieldsEqual || c.WriterFieldsPreserved != (c.Reader == c.Writer) {
			t.Fatal("compatibility result misrepresents metadata loss", c)
		}
	}
	for _, p := range report.Probes {
		if !p.Verified {
			t.Fatal(p.ID)
		}
	}
}

func TestXMLRejectsDuplicateNestedValues(t *testing.T) {
	wire, e := testSuite.Encode("xml", 2, model.Fixture(1, 2))
	if e != nil {
		t.Fatal(e)
	}
	for _, mutation := range [][2]string{
		{"<quantity>1</quantity>", "<quantity>1</quantity><quantity>2</quantity>"},
		{"<label><key>channel</key><value>lab</value></label>", "<label><key>channel</key><value>lab</value></label><label><key>channel</key><value>web</value></label>"},
	} {
		bad := bytes.Replace(wire, []byte(mutation[0]), []byte(mutation[1]), 1)
		if bytes.Equal(bad, wire) {
			t.Fatal("mutation did not reach fixture")
		}
		if _, e = testSuite.Decode(context.Background(), "xml", 2, bad); e == nil {
			t.Fatal("duplicate nested XML value accepted")
		}
	}
}

func TestCommonTextDomainCannotBeSilentlyReplacedByXML(t *testing.T) {
	for _, text := range []string{"invalid\x01text", "invalid\ufffetext", string([]byte{0xff})} {
		r := model.Fixture(1, 2)
		r.Name = text
		for _, format := range Formats {
			if _, e := testSuite.Encode(format, 2, r); e == nil {
				t.Fatal(format, "accepted text outside the shared domain")
			}
		}
	}
	for _, format := range Formats {
		r := model.Fixture(1, 2)
		r.Name = "tab\tline\nreturn\rUnicode 数据 😀"
		wire, e := testSuite.Encode(format, 2, r)
		if e != nil {
			t.Fatal(e)
		}
		got, e := testSuite.Decode(context.Background(), format, 2, wire)
		if e != nil || got.Name != r.Name {
			t.Fatal(format, "changed valid Unicode/whitespace", e)
		}
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	for _, format := range Formats {
		b.Run(format, func(b *testing.B) {
			r := model.Fixture(10, 2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wire, e := testSuite.Encode(format, 2, r)
				if e != nil {
					b.Fatal(e)
				}
				if _, e = testSuite.Decode(context.Background(), format, 2, wire); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
