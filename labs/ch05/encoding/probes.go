package encoding

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	p1 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/protov1"
	p2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/protov2"
	t1 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/thriftv1"
	t2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/thriftv2"
	tw "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/thriftwide"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/apache/thrift/lib/go/thrift"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Probe struct {
	ID          string         `json:"id"`
	Observation string         `json:"observation"`
	Verified    bool           `json:"verified"`
	Values      map[string]any `json:"values,omitempty"`
}

// Probes exercises format semantics independently of the compatible fixture
// matrix. Deliberately incompatible native inputs never bypass public decoders.
func (s *Suite) Probes(ctx context.Context) ([]Probe, error) {
	var out []Probe
	add := func(id, observation string, ok bool, values map[string]any) {
		out = append(out, Probe{id, observation, ok, values})
	}
	r := model.Fixture(1, 2)
	wide := proto2(r)
	wide.Lines[0].Quantity = 4294967297
	wire, e := proto.Marshal(wide)
	if e != nil {
		return nil, e
	}
	old := &p1.Snapshot{}
	e = proto.Unmarshal(wire, old)
	_, guarded := s.Decode(ctx, "protobuf", 1, wire)
	add("protobuf-integer-width", "Same varint wire type permits i64 to i32 narrowing; the business bound rejects it before conversion.", e == nil && len(old.Lines) == 1 && old.Lines[0].Quantity == 1 && guarded != nil, map[string]any{"written": int64(4294967297), "native_old_read": old.GetLines()[0].GetQuantity(), "guard_rejected": guarded != nil})

	oldJSON, e := protojson.Marshal(proto1(model.Fixture(1, 1)))
	if e != nil {
		return nil, e
	}
	// Isolate the renamed field from the separately retired legacy field.
	var fields map[string]json.RawMessage
	if e = json.Unmarshal(oldJSON, &fields); e != nil {
		return nil, e
	}
	delete(fields, "legacyNote")
	oldJSON, _ = json.Marshal(fields)
	newMessage := &p2.Snapshot{}
	jsonError := protojson.Unmarshal(oldJSON, newMessage)
	binaryWire, _ := proto.Marshal(proto1(model.Fixture(1, 1)))
	binaryError := proto.Unmarshal(binaryWire, newMessage)
	add("protobuf-tag-rename", "Tag 3 survives a binary rename; the generated ProtoJSON customerName/displayName change breaks strict JSON reads.", jsonError != nil && binaryError == nil && newMessage.DisplayName == r.Name, nil)
	d := newMessage.ProtoReflect().Descriptor()
	reserved := d.ReservedRanges().Has(5) && d.ReservedNames().Has("legacy_note")
	relayed, e := PreserveProtoRelay(2, binaryWire, "renamed")
	if e != nil {
		return nil, e
	}
	returned := &p1.Snapshot{}
	e = proto.Unmarshal(relayed, returned)
	add("protobuf-retired-tag", "The descriptor reserves tag/name 5; a native generated relay still retains its old unknown value.", reserved && e == nil && returned.LegacyNote == "retired field", map[string]any{"reserved_number": 5, "returned_legacy_note": returned.LegacyNote})

	for _, format := range []string{"thrift-binary", "thrift-compact"} {
		buf := thrift.NewTMemoryBufferLen(256)
		incompatible := &tw.Snapshot{TenantID: r.Tenant, OrderID: r.ID, DisplayName: r.Name, Version: r.Version, Lines: []*tw.Line{{Sku: "sku-000", Quantity: 1}}}
		if e = incompatible.Write(ctx, thriftProtocol(format, buf)); e != nil {
			return nil, e
		}
		toOld := t1.NewSnapshot().Read(ctx, thriftProtocol(format, buf))
		buf.Reset()
		if e = thrift1(model.Fixture(1, 1)).Write(ctx, thriftProtocol(format, buf)); e != nil {
			return nil, e
		}
		toWide := tw.NewSnapshot().Read(ctx, thriftProtocol(format, buf))
		add(format+"-integer-width", "Changing required quantity i32/i64 changes the Thrift type; both readers skip the mismatched field and reject missing required quantity, even for value 1.", toOld != nil && toWide != nil, nil)
		usd := "USD"
		r.Currency = &usd
		wire, e = s.Encode(format, 2, r)
		if e != nil {
			return nil, e
		}
		seen, e := scanThrift(ctx, format, wire, false)
		if e != nil {
			return nil, e
		}
		decoded, e := s.Decode(ctx, format, 2, wire)
		native := t2.NewSnapshot()
		add(format+"-default-presence", "Generated Go currency defaults to USD and omits explicit USD; absent and explicit default collapse, while optional false/zero pointers survive.", e == nil && native.GetCurrency() == "USD" && !native.IsSetCurrency() && seen[6] != thrift.STRING && decoded.Currency == nil && decoded.Expedited != nil && !*decoded.Expedited && decoded.Discount != nil && *decoded.Discount == 0, map[string]any{"native_default": native.GetCurrency(), "currency_emitted": seen[6] == thrift.STRING})
	}
	r = model.Fixture(1, 2)
	wire, e = s.Encode("json", 2, r)
	if e != nil {
		return nil, e
	}
	value, e := scanJSON(wire)
	if e != nil {
		return nil, e
	}
	strictResource, e := jsonschema.UnmarshalJSON(bytes.NewReader(schema1))
	if e != nil {
		return nil, e
	}
	strictResource.(map[string]any)["additionalProperties"] = false
	c := jsonschema.NewCompiler()
	const strictURL = "https://atlas.example/schemas/encoding/strict-old"
	if e = c.AddResource(strictURL, strictResource); e != nil {
		return nil, e
	}
	strict, e := c.Compile(strictURL)
	if e != nil {
		return nil, e
	}
	_, typedError := s.Decode(ctx, "json", 1, wire)
	add("json-schema-open-closed", "The tolerant old schema accepts new fields; additionalProperties=false rejects the same record. Typed acceptance does not preserve unknown metadata.", typedError == nil && strict.Validate(value) != nil, nil)
	loose, exact, e := NumericJSON()
	add("json-integer-precision", "Bare interface numbers use float64; decimal strings or UseNumber preserve integers beyond 2^53.", e == nil && loose == 9007199254740992 && exact == "9007199254740993", map[string]any{"float64_read": loose, "exact_read": exact})
	json.Unmarshal(wire, &fields)
	delete(fields, "expedited")
	absent, _ := json.Marshal(fields)
	fields["expedited"] = json.RawMessage("null")
	null, _ := json.Marshal(fields)
	a, ae := s.Decode(ctx, "json", 2, absent)
	n, ne := s.Decode(ctx, "json", 2, null)
	add("json-null-absence", "The schema permits null; a typed *bool merges null and absence, whereas the raw object retains the difference.", ae == nil && ne == nil && a.Expedited == nil && n.Expedited == nil && !bytes.Equal(absent, null), nil)
	type nativePointer struct{ Value *bool }
	f := false
	var gobBuffer bytes.Buffer
	if e = gob.NewEncoder(&gobBuffer).Encode(nativePointer{&f}); e != nil {
		return nil, e
	}
	var nativeOut nativePointer
	e = gob.NewDecoder(&gobBuffer).Decode(&nativeOut)
	add("gob-zero-presence", "Native gob flattens a false pointer to absence; explicit Set/Value wrappers in the lab retain domain presence.", e == nil && nativeOut.Value == nil, nil)
	gobBuffer.Reset()
	type nativeOld struct{ Count int64 }
	type nativeNew struct{ Count string }
	if e = gob.NewEncoder(&gobBuffer).Encode(nativeOld{1}); e != nil {
		return nil, e
	}
	var changed nativeNew
	add("gob-native-type-change", "A field type change is incompatible in a language-specific serializer; concrete Go type metadata is part of this stream.", gob.NewDecoder(&gobBuffer).Decode(&changed) != nil, nil)
	for _, probe := range out {
		if !probe.Verified {
			return nil, fmt.Errorf("encoding observation failed: %s", probe.ID)
		}
	}
	return out, nil
}
