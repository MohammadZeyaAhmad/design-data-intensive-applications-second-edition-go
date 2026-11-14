package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schema/*.json
var schemaFiles embed.FS

func validateSBOM(raw []byte) error {
	compiler := jsonschema.NewCompiler()
	names, err := fs.Glob(schemaFiles, "schema/*.json")
	if err != nil {
		return err
	}
	for _, name := range names {
		body, err := schemaFiles.ReadFile(name)
		if err != nil {
			return err
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		if err = decoder.Decode(&value); err != nil {
			return err
		}
		id, _ := value.(map[string]any)["$id"].(string)
		if err = compiler.AddResource(id, value); err != nil {
			return err
		}
	}
	schema, err := compiler.Compile("http://cyclonedx.org/schema/bom-1.6.schema.json")
	if err != nil {
		return err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&value); err != nil {
		return err
	}
	if schema.Validate(value) != nil {
		return errors.New("SBOM does not satisfy the pinned CycloneDX 1.6 schema")
	}
	return nil
}
