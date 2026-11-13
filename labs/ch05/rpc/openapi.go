package rpc

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed wire/openapi.json
var openAPISpec []byte
var compileOnce sync.Once
var restSchemas map[string]*jsonschema.Schema
var compileError error

func validateREST(name string, raw []byte) error {
	compileOnce.Do(func() {
		var document any
		d := json.NewDecoder(bytes.NewReader(openAPISpec))
		d.UseNumber()
		if e := d.Decode(&document); e != nil {
			compileError = e
			return
		}
		c := jsonschema.NewCompiler()
		c.AssertFormat()
		if e := c.AddResource("https://atlas.invalid/openapi.json", document); e != nil {
			compileError = e
			return
		}
		restSchemas = map[string]*jsonschema.Schema{}
		for _, n := range []string{"ReserveRequest", "ReceiveRequest", "ReserveOutcome", "ReceiveOutcome", "Stock", "Reservation", "Problem", "Health"} {
			s, e := c.Compile("https://atlas.invalid/openapi.json#/components/schemas/" + n)
			if e != nil {
				compileError = e
				return
			}
			restSchemas[n] = s
		}
	})
	if compileError != nil {
		return compileError
	}
	s, ok := restSchemas[name]
	if !ok {
		return errors.New("unknown REST schema")
	}
	if len(raw) > 64<<10 {
		return errors.New("REST value exceeds bound")
	}
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if e := d.Decode(&value); e != nil {
		return e
	}
	return s.Validate(value)
}
