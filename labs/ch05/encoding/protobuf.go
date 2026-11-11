package encoding

import (
	v1 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/protov1"
	v2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/protov2"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func proto1(r model.Record) *v1.Snapshot {
	p := &v1.Snapshot{TenantId: r.Tenant, OrderId: r.ID, CustomerName: r.Name, Version: r.Version, Expedited: r.Expedited, LegacyNote: r.LegacyNote}
	for _, l := range r.Lines {
		p.Lines = append(p.Lines, &v1.Line{Sku: l.SKU, Quantity: int32(l.Quantity)})
	}
	return p
}
func proto2(r model.Record) *v2.Snapshot {
	p := &v2.Snapshot{TenantId: r.Tenant, OrderId: r.ID, DisplayName: r.Name, Version: r.Version, Expedited: r.Expedited, Currency: r.Currency, DiscountMinor: r.Discount, Labels: r.Labels}
	for _, l := range r.Lines {
		p.Lines = append(p.Lines, &v2.Line{Sku: l.SKU, Quantity: l.Quantity})
	}
	return p
}
func protoEncode(version int, r model.Record) ([]byte, error) {
	if version == 1 {
		return proto.MarshalOptions{Deterministic: true}.Marshal(proto1(r))
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(proto2(r))
}
func scanProto(b []byte) error {
	fields, lines := 0, 0
	var scan func([]byte, int) error
	scan = func(data []byte, role int) error {
		for len(data) > 0 {
			fields++
			if fields > 512 {
				return model.ErrBudget
			}
			tag, typ, n := protowire.ConsumeTag(data)
			if n < 0 || tag < 1 {
				return model.ErrInvalid
			}
			data = data[n:]
			if typ == protowire.StartGroupType || typ == protowire.EndGroupType {
				return model.ErrInvalid
			}
			if typ == protowire.BytesType {
				value, n := protowire.ConsumeBytes(data)
				if n < 0 {
					return model.ErrInvalid
				}
				if role == 0 && tag == 4 {
					lines++
					if lines > model.MaxLines {
						return model.ErrBudget
					}
					if e := scan(value, 1); e != nil {
						return e
					}
				} else if role == 0 && tag == 10 {
					if e := scan(value, 2); e != nil {
						return e
					}
				}
				data = data[n:]
				continue
			}
			if role == 1 && tag == 2 && typ == protowire.VarintType {
				v, n := protowire.ConsumeVarint(data)
				if n < 0 || v < 1 || v > 1000000000 {
					return model.ErrInvalid
				}
			}
			n = protowire.ConsumeFieldValue(tag, typ, data)
			if n < 0 {
				return model.ErrInvalid
			}
			data = data[n:]
		}
		return nil
	}
	return scan(b, 0)
}
func protoDecode(version int, b []byte) (model.Record, error) {
	var r model.Record
	if e := scanProto(b); e != nil {
		return r, e
	}
	opt := proto.UnmarshalOptions{RecursionLimit: 8}
	if version == 1 {
		p := &v1.Snapshot{}
		if opt.Unmarshal(b, p) != nil {
			return r, model.ErrInvalid
		}
		r = model.Record{Tenant: p.TenantId, ID: p.OrderId, Name: p.CustomerName, Version: p.Version, Expedited: p.Expedited, LegacyNote: p.LegacyNote, Lines: []model.Line{}}
		for _, l := range p.Lines {
			r.Lines = append(r.Lines, model.Line{SKU: l.Sku, Quantity: int64(l.Quantity)})
		}
	} else {
		p := &v2.Snapshot{}
		if opt.Unmarshal(b, p) != nil {
			return r, model.ErrInvalid
		}
		r = model.Record{Tenant: p.TenantId, ID: p.OrderId, Name: p.DisplayName, Version: p.Version, Expedited: p.Expedited, Currency: p.Currency, Discount: p.DiscountMinor, Labels: p.Labels, Lines: []model.Line{}}
		for _, l := range p.Lines {
			r.Lines = append(r.Lines, model.Line{SKU: l.Sku, Quantity: l.Quantity})
		}
	}
	return r, nil
}
