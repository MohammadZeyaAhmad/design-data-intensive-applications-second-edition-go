package avrolab

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

// Column is a bounded snapshot of SQL catalog metadata, not a live SQL driver.
// The source controls ordinal order; query/result map iteration never does.
type Column struct {
	Name     string `json:"name"`
	SQLType  string `json:"sql_type"`
	Nullable bool   `json:"nullable"`
	Ordinal  int    `json:"ordinal"`
}

var columnName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}$`)

func DynamicSchema(columns []Column) ([]byte, error) {
	if len(columns) < 1 || len(columns) > 32 {
		return nil, avroprofile.ErrBudget
	}
	ordered := append([]Column(nil), columns...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Ordinal < ordered[j].Ordinal })
	root := &avro.SchemaNode{Type: "record", Name: "CatalogExport", Namespace: "atlas.dynamic"}
	names := map[string]bool{}
	ordinals := map[int]bool{}
	for _, column := range ordered {
		if !columnName.MatchString(column.Name) || column.Ordinal < 1 || column.Ordinal > 1024 || names[column.Name] || ordinals[column.Ordinal] {
			return nil, avroprofile.ErrInvalid
		}
		names[column.Name] = true
		ordinals[column.Ordinal] = true
		kind := ""
		switch column.SQLType {
		case "text":
			kind = "string"
		case "integer":
			kind = "int"
		case "bigint":
			kind = "long"
		case "boolean":
			kind = "boolean"
		default:
			return nil, fmt.Errorf("unsupported catalog column type: %s", column.SQLType)
		}
		field := avro.SchemaField{Name: column.Name, Type: avro.SchemaNode{Type: kind}}
		if column.Nullable {
			field.Type = avro.SchemaNode{Type: "union", Branches: []avro.SchemaNode{{Type: "null"}, {Type: kind}}}
			field.HasDefault = true
			field.Default = nil
		}
		root.Fields = append(root.Fields, field)
	}
	s, e := root.Schema()
	if e != nil {
		return nil, e
	}
	_, _, full, e := avroprofile.Compile([]byte(s.String()))
	return full, e
}

type DynamicResult struct {
	Old                 SchemaInfo `json:"old_schema"`
	New                 SchemaInfo `json:"new_schema"`
	StableMetadataOrder bool       `json:"stable_metadata_order"`
	NewNullableDefault  bool       `json:"new_nullable_default"`
	PromotionValue      int64      `json:"promoted_quantity"`
	ReverseRejected     bool       `json:"reverse_rejected"`
}

func dynamicScenario() (DynamicResult, error) {
	oldColumns := []Column{{"sku", "text", false, 1}, {"quantity", "integer", false, 2}}
	newColumns := []Column{{"display_name", "text", true, 3}, {"quantity", "bigint", false, 2}, {"sku", "text", false, 1}}
	old, e := DynamicSchema(oldColumns)
	if e != nil {
		return DynamicResult{}, e
	}
	current, e := DynamicSchema(newColumns)
	if e != nil {
		return DynamicResult{}, e
	}
	shuffled := []Column{newColumns[2], newColumns[0], newColumns[1]}
	again, e := DynamicSchema(shuffled)
	if e != nil {
		return DynamicResult{}, e
	}
	r := NewRegistry()
	oldInfo, e := r.Register("catalog_old", old)
	if e != nil {
		return DynamicResult{}, e
	}
	newInfo, e := r.Register("catalog_new", current)
	if e != nil {
		return DynamicResult{}, e
	}
	writer, _ := r.byVersion("catalog_old")
	reader, _ := r.byVersion("catalog_new")
	wire, e := writer.schema.Encode(map[string]any{"sku": "sku-000", "quantity": int32(7)})
	if e != nil {
		return DynamicResult{}, e
	}
	if e = avroprofile.Scan(writer.plan, wire); e != nil {
		return DynamicResult{}, e
	}
	resolved, e := avro.Resolve(writer.schema, reader.schema)
	if e != nil {
		return DynamicResult{}, e
	}
	var value any
	rest, e := resolved.Decode(wire, &value)
	if e != nil || len(rest) > 0 {
		return DynamicResult{}, avroprofile.ErrInvalid
	}
	out, ok := value.(map[string]any)
	if !ok {
		return DynamicResult{}, avroprofile.ErrInvalid
	}
	_, present := out["display_name"]
	quantity, ok := out["quantity"].(int64)
	result := DynamicResult{oldInfo, newInfo, string(again) == string(current), present && out["display_name"] == nil, quantity, avro.CheckCompatibility(reader.schema, writer.schema) != nil}
	if !ok || !result.StableMetadataOrder || !result.NewNullableDefault || result.PromotionValue != 7 || !result.ReverseRejected {
		return DynamicResult{}, avroprofile.ErrInvalid
	}
	return result, nil
}
