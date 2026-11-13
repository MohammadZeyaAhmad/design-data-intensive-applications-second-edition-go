package contracts

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed asyncapi.json
var specification []byte
var once sync.Once
var schemas map[string]*jsonschema.Schema
var compileError error

func Validate(name string, raw []byte) error {
	once.Do(func() {
		var doc any
		d := json.NewDecoder(bytes.NewReader(specification))
		d.UseNumber()
		if e := d.Decode(&doc); e != nil {
			compileError = e
			return
		}
		compiler := jsonschema.NewCompiler()
		if e := compiler.AddResource("https://atlas.invalid/actors.json", doc); e != nil {
			compileError = e
			return
		}
		schemas = map[string]*jsonschema.Schema{}
		for _, name := range []string{"Command", "Response"} {
			schema, e := compiler.Compile("https://atlas.invalid/actors.json#/components/schemas/" + name)
			if e != nil {
				compileError = e
				return
			}
			schemas[name] = schema
		}
	})
	if compileError != nil {
		return compileError
	}
	schema, ok := schemas[name]
	if !ok || len(raw) > 8192 {
		return errors.New("unknown or oversized actor contract value")
	}
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if e := d.Decode(&value); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing contract value")
	}
	return schema.Validate(value)
}
