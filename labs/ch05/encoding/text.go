package encoding

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"sync"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vmihailenco/msgpack/v5"
)

//go:embed generated/schema/v1.schema.json
var schema1 []byte

//go:embed generated/schema/v2.schema.json
var schema2 []byte
var schemaOnce sync.Once
var schemas [2]*jsonschema.Schema
var schemaError error

func compileSchemas() {
	c := jsonschema.NewCompiler()
	for i, b := range [][]byte{schema1, schema2} {
		var v any
		v, schemaError = jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if schemaError != nil {
			return
		}
		url := fmt.Sprintf("https://atlas.example/schemas/encoding/v%d", i+1)
		schemaError = c.AddResource(url, v)
		if schemaError != nil {
			return
		}
		schemas[i], schemaError = c.Compile(url)
		if schemaError != nil {
			return
		}
	}
}
func ValidateJSON(version int, b []byte) error {
	if version != 1 && version != 2 {
		return model.ErrInvalid
	}
	v, e := scanJSON(b)
	if e != nil {
		return e
	}
	schemaOnce.Do(compileSchemas)
	if schemaError != nil {
		return schemaError
	}
	if e = schemas[version-1].Validate(v); e != nil {
		return model.ErrInvalid
	}
	return nil
}
func textEncode(kind string, version int, r model.Record) ([]byte, error) {
	var v any
	if version == 1 {
		v = to1(r)
	} else {
		v = to2(r)
	}
	switch kind {
	case "json":
		return json.Marshal(v)
	case "xml":
		if version == 2 {
			v = toXML2(r)
		}
		return xml.Marshal(v)
	case "messagepack":
		var b bytes.Buffer
		e := msgpack.NewEncoder(&b)
		e.SetSortMapKeys(true)
		err := e.Encode(v)
		return b.Bytes(), err
	}
	return nil, model.ErrInvalid
}
func textDecode(kind string, version int, b []byte) (model.Record, error) {
	var one wire1
	var two wire2
	var r model.Record
	var e error
	switch kind {
	case "json":
		if e = ValidateJSON(version, b); e != nil {
			return r, e
		}
		if version == 1 {
			e = json.Unmarshal(b, &one)
			r = one.record()
		} else {
			e = json.Unmarshal(b, &two)
			r = two.record()
		}
	case "xml":
		if e = scanXML(b); e != nil {
			return r, e
		}
		d := xml.NewDecoder(bytes.NewReader(b))
		if version == 1 {
			e = d.Decode(&one)
			r = one.record()
		} else {
			var w xml2
			e = d.Decode(&w)
			if len(w.LabelList) > 8 {
				return r, model.ErrBudget
			}
			seen := map[string]bool{}
			for _, label := range w.LabelList {
				if seen[label.Key] {
					return r, model.ErrInvalid
				}
				seen[label.Key] = true
			}
			r = w.record()
		}
	case "messagepack":
		if e = scanMessagePack(b); e != nil {
			return r, e
		}
		// Validate the wide shape before the i32 reader can narrow an integer.
		d := msgpack.NewDecoder(bytes.NewReader(b))
		if e = d.Decode(&two); e != nil {
			return r, model.ErrInvalid
		}
		if e = model.Validate(two.record()); e != nil {
			return r, e
		}
		if version == 1 {
			e = msgpack.Unmarshal(b, &one)
			r = one.record()
		} else {
			r = two.record()
		}
	default:
		return r, model.ErrInvalid
	}
	if e != nil {
		return model.Record{}, model.ErrInvalid
	}
	return r, nil
}

func PreserveJSONRelay(b []byte, name string) ([]byte, error) {
	if e := ValidateJSON(1, b); e != nil {
		return nil, e
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil {
		return nil, model.ErrInvalid
	}
	fields["name"], _ = json.Marshal(name)
	out, e := json.Marshal(fields)
	if e != nil {
		return nil, e
	}
	if e = ValidateJSON(1, out); e != nil {
		return nil, e
	}
	return out, nil
}

// NumericJSON demonstrates the default float64 interface decoding versus an
// exact json.Number path. These helpers operate on tiny fixed probe inputs.
func NumericJSON() (float64, string, error) {
	b := []byte(`{"version":9007199254740993}`)
	var loose map[string]any
	if e := json.Unmarshal(b, &loose); e != nil {
		return 0, "", e
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var exact map[string]any
	if e := d.Decode(&exact); e != nil {
		return 0, "", e
	}
	if _, e := d.Token(); e != io.EOF {
		return 0, "", model.ErrInvalid
	}
	return loose["version"].(float64), string(exact["version"].(json.Number)), nil
}
