package avroprofile_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

func TestIndependentAvroGoldenRecordAndArrayBlocks(t *testing.T) {
	schema, plan, _, e := avroprofile.Compile([]byte(`{"type":"record","name":"Pair","fields":[{"name":"n","type":"long"},{"name":"s","type":"string"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	// Zigzag(27)=54, UTF-8 length(3)=6, then the three literal bytes.
	expected := []byte{54, 6, 'f', 'o', 'o'}
	wire, e := schema.Encode(map[string]any{"n": int64(27), "s": "foo"})
	if e != nil || !bytes.Equal(wire, expected) || avroprofile.Scan(plan, expected) != nil {
		t.Fatal("wire diverged from the independent spec vector", wire, e)
	}
	schema, plan, _, e = avroprofile.Compile([]byte(`{"type":"array","items":"int"}`))
	if e != nil {
		t.Fatal(e)
	}
	for _, wire := range [][]byte{{3, 4, 2, 4, 0}, {2, 2, 2, 4, 0}} { // -2 + byte length 2; or two positive blocks of one.
		if e = avroprofile.Scan(plan, wire); e != nil {
			t.Fatal(e)
		}
		var got any
		rest, e := schema.Decode(wire, &got)
		if e != nil || len(rest) > 0 || !reflect.DeepEqual(got, []any{int32(1), int32(2)}) {
			t.Fatal(got, e)
		}
	}
	if avroprofile.Scan(plan, []byte{3, 6, 2, 4, 0}) == nil {
		t.Fatal("negative block byte length was ignored")
	}
}
func TestProfileRejectsAmbiguousMapKeysAndNoncanonicalBoolean(t *testing.T) {
	_, plan, _, e := avroprofile.Compile([]byte(`{"type":"map","values":"string"}`))
	if e != nil {
		t.Fatal(e)
	}
	// Two map blocks repeat a key; a generic decoder may retain the last value.
	if avroprofile.Scan(plan, []byte{2, 2, 'a', 2, 'x', 2, 2, 'a', 2, 'y', 0}) == nil {
		t.Fatal("duplicate map key across blocks accepted")
	}
	_, plan, _, e = avroprofile.Compile([]byte(`"boolean"`))
	if e != nil {
		t.Fatal(e)
	}
	if avroprofile.Scan(plan, []byte{2}) == nil {
		t.Fatal("non-0/1 boolean accepted by stricter public profile")
	}
}

func TestSchemaNumbersCannotRequestUnboundedPrecision(t *testing.T) {
	for _, schema := range []string{`{"type":"record","name":"R","fields":[{"name":"n","type":"long","default":1e999999999}]}`, `{"type":"record","name":"R","fields":[{"name":"n","type":"bytes","logicalType":"decimal","precision":2000000000}]}`} {
		if _, _, _, e := avroprofile.Compile([]byte(schema)); e == nil {
			t.Fatal("out-of-profile numeric/logical metadata accepted")
		}
	}
}

func TestNormalizationPreservesEmptyArrayReaderDefaults(t *testing.T) {
	writer, _, _, e := avroprofile.Compile([]byte(`{"type":"record","name":"R","fields":[{"name":"name","type":"string"}]}`))
	if e != nil {
		t.Fatal(e)
	}
	reader, _, full, e := avroprofile.Compile([]byte(`{"type":"record","name":"R","aliases":[],"fields":[{"name":"name","type":"string"},{"name":"tags","type":{"type":"array","items":"string"},"default":[]}]}`))
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(full, []byte(`"default":[]`)) || !bytes.Contains(full, []byte(`"aliases":[]`)) {
		t.Fatal("normalization changed empty arrays to null", string(full))
	}
	resolved, e := avro.Resolve(writer, reader)
	if e != nil {
		t.Fatal(e)
	}
	wire, e := writer.Encode(map[string]any{"name": "order"})
	if e != nil {
		t.Fatal(e)
	}
	var value any
	rest, e := resolved.Decode(wire, &value)
	if e != nil || len(rest) != 0 {
		t.Fatal(e)
	}
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatal(value)
	}
	tags, ok := result["tags"].([]any)
	if !ok || len(tags) != 0 || result["name"] != "order" {
		t.Fatal("empty reader array default failed", result)
	}
}
