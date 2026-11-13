package avrolab

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

type Evolution struct {
	ID                 string `json:"id"`
	Compatible         bool   `json:"compatible"`
	ExpectedCompatible bool   `json:"expected_compatible"`
	Decoded            string `json:"decoded_value,omitempty"`
	Detail             string `json:"detail"`
}
type evolutionCase struct {
	id, writer, reader string
	input, expected    any
	compatible         bool
	detail             string
}

func evolutionCases() []evolutionCase {
	base := `{"type":"record","name":"R","fields":[{"name":"name","type":"string"}]}`
	added := `{"type":"record","name":"R","fields":[{"name":"active","type":"boolean","default":false},{"name":"name","type":"string"}]}`
	required := `{"type":"record","name":"R","fields":[{"name":"active","type":"boolean"},{"name":"name","type":"string"}]}`
	alias := `{"type":"record","name":"R","fields":[{"name":"display","aliases":["name"],"type":"string"}]}`
	renamed := `{"type":"record","name":"R","fields":[{"name":"display","type":"string"}]}`
	typeAlias := `{"type":"record","name":"NewR","aliases":["R"],"fields":[{"name":"name","type":"string"}]}`
	oldMap := map[string]any{"name": "order"}
	newMap := map[string]any{"active": false, "name": "order"}
	return []evolutionCase{
		{"add-default-forward", base, added, oldMap, newMap, true, "Missing writer field receives the reader default; reader field order can differ."},
		{"add-default-reverse", added, base, newMap, oldMap, true, "Writer-only fields are skipped."},
		{"add-required-forward", base, required, oldMap, nil, false, "A reader-only field without a default is incompatible."},
		{"remove-required-reverse", required, base, newMap, oldMap, true, "Removing a field from a reader is allowed."},
		{"field-alias-forward", base, alias, oldMap, map[string]any{"display": "order"}, true, "Reader aliases map the writer name."},
		{"field-alias-reverse", alias, base, map[string]any{"display": "order"}, nil, false, "An alias in the writer does not update an old reader."},
		{"rename-without-alias", base, renamed, oldMap, nil, false, "Record field names, rather than numeric tags, identify fields."},
		{"record-alias-forward", base, typeAlias, oldMap, oldMap, true, "A reader named-type alias accepts the old record fullname."},
		{"record-alias-reverse", typeAlias, base, oldMap, nil, false, "Writer named-type aliases do not establish reverse compatibility."},
		{"int-long", `"int"`, `"long"`, int32(7), int64(7), true, "Promotion preserves this integer exactly."},
		{"long-int", `"long"`, `"int"`, int64(7), nil, false, "Narrowing is rejected even when the current value fits."},
		{"long-double", `"long"`, `"double"`, int64(9007199254740993), float64(9007199254740992), true, "Permitted promotion can round an integer beyond 2^53."},
		{"double-long", `"double"`, `"long"`, float64(7), nil, false, "Numeric promotion is directional."},
		{"float-double", `"float"`, `"double"`, float32(1.5), float64(1.5), true, "Wider floating representation preserves this value."},
		{"double-float", `"double"`, `"float"`, float64(1.5), nil, false, "Avro schema resolution rejects this narrowing."},
		{"string-bytes", `"string"`, `"bytes"`, "数据", []byte("数据"), true, "String-to-bytes promotion uses UTF-8 bytes."},
		{"bytes-string", `"bytes"`, `"string"`, []byte("数据"), "数据", true, "This valid UTF-8 byte value promotes to string."},
		{"union-add", `["null","int"]`, `["string","long","null"]`, int32(7), int64(7), true, "Reader unions remap the writer branch index and can promote its type."},
		{"union-remove", `["null","int"]`, `"long"`, int32(7), nil, false, "Pinned runtime rejects any unmatched writer branch up front, even if this datum selected int; this is a conservative whole-schema policy."},
		{"reader-union", `"int"`, `["double","int"]`, int32(7), int32(7), true, "Pinned runtime chooses an exact reader branch before a promoted branch."},
		{"array-promotion", `{"type":"array","items":"int"}`, `{"type":"array","items":"long"}`, []any{int32(1), int32(2)}, []any{int64(1), int64(2)}, true, "Array item resolution applies recursively."},
		{"map-promotion", `{"type":"map","values":"int"}`, `{"type":"map","values":"long"}`, map[string]any{"a": int32(1)}, map[string]any{"a": int64(1)}, true, "Map value resolution applies recursively."},
		{"enum-fallback", `{"type":"enum","name":"E","symbols":["NEW"]}`, `{"type":"enum","name":"E","symbols":["UNKNOWN"],"default":"UNKNOWN"}`, "NEW", "UNKNOWN", true, "A reader enum default handles a writer symbol it does not know."},
		{"enum-no-fallback", `{"type":"enum","name":"E","symbols":["NEW"]}`, `{"type":"enum","name":"E","symbols":["OLD"]}`, "NEW", nil, false, "Unknown writer symbols without reader fallback are rejected."},
		{"fixed-same", `{"type":"fixed","name":"F","size":2}`, `{"type":"fixed","name":"F","size":2}`, []byte{1, 2}, []byte{1, 2}, true, "A matching named fixed type retains its exact byte width."},
		{"fixed-size-change", `{"type":"fixed","name":"F","size":2}`, `{"type":"fixed","name":"F","size":3}`, []byte{1, 2}, nil, false, "Fixed type width is part of compatibility."},
	}
}
func EvolutionCases() ([]Evolution, error) {
	var out []Evolution
	for _, trial := range evolutionCases() {
		writer, plan, _, e := avroprofile.Compile([]byte(trial.writer))
		if e != nil {
			return nil, fmt.Errorf("%s writer: %w", trial.id, e)
		}
		reader, _, _, e := avroprofile.Compile([]byte(trial.reader))
		if e != nil {
			return nil, e
		}
		wire, e := writer.Encode(trial.input)
		if e != nil {
			return nil, e
		}
		if e = avroprofile.Scan(plan, wire); e != nil {
			return nil, e
		}
		compatible := avro.CheckCompatibility(writer, reader) == nil
		resolved, e := avro.Resolve(writer, reader)
		if compatible != trial.compatible || (e == nil) != trial.compatible {
			return nil, fmt.Errorf("unexpected compatibility: %s check=%t resolve=%v", trial.id, compatible, e)
		}
		result := Evolution{trial.id, compatible, trial.compatible, "", trial.detail}
		if e == nil {
			var value any
			rest, e := resolved.Decode(wire, &value)
			if e != nil || len(rest) != 0 || !equalNative(value, trial.expected) {
				return nil, fmt.Errorf("unexpected %s value: %#v expected %#v: %v", trial.id, value, trial.expected, e)
			}
			result.Decoded = fmt.Sprintf("%#v", value)
		}
		out = append(out, result)
	}
	return out, nil
}
func equalNative(a, b any) bool {
	if aBytes, ok := a.([]byte); ok {
		bBytes, ok := b.([]byte)
		return ok && bytes.Equal(aBytes, bBytes)
	}
	return reflect.DeepEqual(a, b)
}
