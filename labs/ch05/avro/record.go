package avrolab

import (
	"context"
	"fmt"

	g1 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/avro/generated/v1"
	g2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/avro/generated/v2"
	g3 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/avro/generated/v3"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

func fixture(lines int, version string) model.Record {
	v := 2
	if version == "v1" {
		v = 1
	}
	return model.Fixture(lines, v)
}

func generated(version string, r model.Record) (any, error) {
	switch version {
	case "v1":
		v := &g1.OrderSnapshot{TenantId: r.Tenant, OrderId: r.ID, CustomerName: r.Name, Version: r.Version, Expedited: r.Expedited, Lines: []g1.Line{}}
		if r.LegacyNote != "" {
			note := r.LegacyNote
			v.LegacyNote = &note
		}
		for _, l := range r.Lines {
			v.Lines = append(v.Lines, g1.Line{Sku: l.SKU, Quantity: int32(l.Quantity)})
		}
		return v, nil
	case "v2":
		v := &g2.OrderSnapshot{TenantId: r.Tenant, OrderId: r.ID, CustomerName: r.Name, Version: r.Version, Expedited: r.Expedited, DiscountMinor: r.Discount, Labels: r.Labels, Lines: []g2.Line{}, Currency: "USD"}
		if r.Currency != nil {
			v.Currency = *r.Currency
		}
		for _, l := range r.Lines {
			v.Lines = append(v.Lines, g2.Line{Sku: l.SKU, Quantity: int32(l.Quantity)})
		}
		return v, nil
	case "v3":
		v := &g3.OrderSnapshot{TenantId: r.Tenant, OrderId: r.ID, DisplayName: r.Name, Version: r.Version, Expedited: r.Expedited, DiscountMinor: r.Discount, Labels: r.Labels, Lines: []g3.Line{}, Currency: "EUR"}
		if r.Currency != nil {
			v.Currency = *r.Currency
		}
		for _, l := range r.Lines {
			v.Lines = append(v.Lines, g3.Line{Sku: l.SKU, Quantity: l.Quantity})
		}
		return v, nil
	}
	return nil, avroprofile.ErrInvalid
}
func dynamic(version string, r model.Record) (any, error) {
	m := map[string]any{"tenant_id": r.Tenant, "order_id": r.ID, "version": r.Version, "expedited": r.Expedited}
	name := "customer_name"
	if version == "v3" {
		name = "display_name"
	}
	m[name] = r.Name
	lines := []any{}
	for _, l := range r.Lines {
		var qty any = int32(l.Quantity)
		if version == "v3" {
			qty = l.Quantity
		}
		lines = append(lines, map[string]any{"sku": l.SKU, "quantity": qty})
	}
	m["lines"] = lines
	if version == "v1" {
		var legacy *string
		if r.LegacyNote != "" {
			text := r.LegacyNote
			legacy = &text
		}
		m["legacy_note"] = legacy
	} else if version == "v2" || version == "v3" {
		currency := "USD"
		if version == "v3" {
			currency = "EUR"
		}
		if r.Currency != nil {
			currency = *r.Currency
		}
		m["currency"] = currency
		m["discount_minor"] = r.Discount
		labels := r.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		m["labels"] = labels
	} else {
		return nil, avroprofile.ErrInvalid
	}
	return m, nil
}
func input(mode, version string, r model.Record) (any, error) {
	if e := model.Validate(r); e != nil {
		return nil, e
	}
	switch mode {
	case "generated":
		return generated(version, r)
	case "dynamic":
		return dynamic(version, r)
	}
	return nil, avroprofile.ErrInvalid
}

func readGenerated(s *avro.Schema, version string, b []byte) (model.Record, error) {
	var r model.Record
	var rest []byte
	var e error
	switch version {
	case "v1":
		v := &g1.OrderSnapshot{}
		rest, e = s.Decode(b, v)
		r = model.Record{Tenant: v.TenantId, ID: v.OrderId, Name: v.CustomerName, Version: v.Version, Expedited: v.Expedited, Lines: []model.Line{}}
		if v.LegacyNote != nil {
			r.LegacyNote = *v.LegacyNote
		}
		for _, l := range v.Lines {
			r.Lines = append(r.Lines, model.Line{SKU: l.Sku, Quantity: int64(l.Quantity)})
		}
	case "v2":
		v := &g2.OrderSnapshot{}
		rest, e = s.Decode(b, v)
		r = model.Record{Tenant: v.TenantId, ID: v.OrderId, Name: v.CustomerName, Version: v.Version, Expedited: v.Expedited, Currency: &v.Currency, Discount: v.DiscountMinor, Labels: v.Labels, Lines: []model.Line{}}
		for _, l := range v.Lines {
			r.Lines = append(r.Lines, model.Line{SKU: l.Sku, Quantity: int64(l.Quantity)})
		}
	case "v3":
		v := &g3.OrderSnapshot{}
		rest, e = s.Decode(b, v)
		r = model.Record{Tenant: v.TenantId, ID: v.OrderId, Name: v.DisplayName, Version: v.Version, Expedited: v.Expedited, Currency: &v.Currency, Discount: v.DiscountMinor, Labels: v.Labels, Lines: []model.Line{}}
		for _, l := range v.Lines {
			r.Lines = append(r.Lines, model.Line{SKU: l.Sku, Quantity: l.Quantity})
		}
	default:
		return r, avroprofile.ErrInvalid
	}
	if e != nil || len(rest) != 0 {
		return model.Record{}, avroprofile.ErrInvalid
	}
	return r, nil
}
func readDynamic(s *avro.Schema, version string, b []byte) (model.Record, error) {
	var value any
	rest, e := s.Decode(b, &value)
	if e != nil || len(rest) != 0 {
		return model.Record{}, avroprofile.ErrInvalid
	}
	return recordFromMap(version, value)
}
func recordFromMap(version string, value any) (model.Record, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return model.Record{}, avroprofile.ErrInvalid
	}
	// Fresh typed values avoid stale map fields surviving a reused destination.
	r := model.Record{Lines: []model.Line{}}
	r.Tenant, _ = m["tenant_id"].(string)
	r.ID, _ = m["order_id"].(string)
	name := "customer_name"
	if version == "v3" {
		name = "display_name"
	}
	r.Name, _ = m[name].(string)
	r.Version, _ = m["version"].(int64)
	if v, ok := m["expedited"].(bool); ok {
		r.Expedited = &v
	}
	if v, ok := m["discount_minor"].(int64); ok {
		r.Discount = &v
	}
	if v, ok := m["currency"].(string); ok {
		r.Currency = &v
	}
	r.LegacyNote, _ = m["legacy_note"].(string)
	if labels, ok := m["labels"].(map[string]any); ok {
		r.Labels = map[string]string{}
		for k, v := range labels {
			label, ok := v.(string)
			if !ok {
				return model.Record{}, avroprofile.ErrInvalid
			}
			r.Labels[k] = label
		}
	}
	lines, ok := m["lines"].([]any)
	if !ok {
		return model.Record{}, avroprofile.ErrInvalid
	}
	for _, line := range lines {
		l, ok := line.(map[string]any)
		if !ok {
			return model.Record{}, avroprofile.ErrInvalid
		}
		sku, _ := l["sku"].(string)
		var qty int64
		switch n := l["quantity"].(type) {
		case int32:
			qty = int64(n)
		case int64:
			qty = n
		default:
			return model.Record{}, avroprofile.ErrInvalid
		}
		r.Lines = append(r.Lines, model.Line{SKU: sku, Quantity: qty})
	}
	return r, nil
}
func (r *Registry) Encode(ctx context.Context, version, mode string, record model.Record) ([]byte, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	writer, e := r.byVersion(version)
	if e != nil {
		return nil, e
	}
	v, e := input(mode, version, record)
	if e != nil {
		return nil, e
	}
	wire, e := writer.schema.Encode(v)
	if e != nil {
		return nil, avroprofile.ErrInvalid
	}
	if e = avroprofile.Scan(writer.plan, wire); e != nil {
		return nil, e
	}
	return wire, nil
}
func decode(ctx context.Context, writer *schemaEntry, reader *schemaEntry, readerVersion, mode string, wire []byte) (model.Record, error) {
	if e := ctx.Err(); e != nil {
		return model.Record{}, e
	}
	if e := avroprofile.Scan(writer.plan, wire); e != nil {
		return model.Record{}, e
	}
	resolved, e := avro.Resolve(writer.schema, reader.schema)
	if e != nil {
		return model.Record{}, fmt.Errorf("%w: %v", ErrIncompatible, e)
	}
	var record model.Record
	switch mode {
	case "generated":
		record, e = readGenerated(resolved, readerVersion, wire)
	case "dynamic":
		record, e = readDynamic(resolved, readerVersion, wire)
	default:
		e = avroprofile.ErrInvalid
	}
	if e != nil {
		return model.Record{}, e
	}
	if e = model.Validate(record); e != nil {
		return model.Record{}, e
	}
	return record, nil
}
func (r *Registry) Decode(ctx context.Context, writerDigest, readerVersion, mode string, wire []byte) (model.Record, error) {
	writer, e := r.byDigest(writerDigest)
	if e != nil {
		return model.Record{}, e
	}
	reader, e := r.byVersion(readerVersion)
	if e != nil {
		return model.Record{}, e
	}
	return decode(ctx, writer, reader, readerVersion, mode, wire)
}
