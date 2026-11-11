package encoding

import (
	"context"

	v1 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/thriftv1"
	v2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/thriftv2"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/apache/thrift/lib/go/thrift"
)

func thriftProtocol(format string, t thrift.TTransport) thrift.TProtocol {
	strict := true
	c := &thrift.TConfiguration{MaxMessageSize: model.MaxWire, MaxFrameSize: model.MaxWire, TBinaryStrictRead: &strict, TBinaryStrictWrite: &strict}
	if format == "thrift-compact" {
		return thrift.NewTCompactProtocolConf(t, c)
	}
	return thrift.NewTBinaryProtocolConf(t, c)
}
func thrift1(r model.Record) *v1.Snapshot {
	p := v1.NewSnapshot()
	p.TenantID, p.OrderID, p.CustomerName, p.Version, p.Expedited = r.Tenant, r.ID, r.Name, r.Version, r.Expedited
	p.Lines = []*v1.Line{}
	if r.LegacyNote != "" {
		s := r.LegacyNote
		p.LegacyNote = &s
	}
	for _, l := range r.Lines {
		p.Lines = append(p.Lines, &v1.Line{Sku: l.SKU, Quantity: int32(l.Quantity)})
	}
	return p
}
func thrift2(r model.Record) *v2.Snapshot {
	p := v2.NewSnapshot()
	p.TenantID, p.OrderID, p.DisplayName, p.Version, p.Expedited = r.Tenant, r.ID, r.Name, r.Version, r.Expedited
	p.Lines = []*v2.Line{}
	if r.Currency != nil {
		p.Currency = *r.Currency
	}
	p.DiscountMinor, p.Labels = r.Discount, r.Labels
	for _, l := range r.Lines {
		p.Lines = append(p.Lines, &v2.Line{Sku: l.SKU, Quantity: int32(l.Quantity)})
	}
	return p
}
func thriftEncode(format string, version int, r model.Record) ([]byte, error) {
	t := thrift.NewTMemoryBufferLen(1024)
	p := thriftProtocol(format, t)
	var e error
	if version == 1 {
		e = thrift1(r).Write(context.Background(), p)
	} else {
		e = thrift2(r).Write(context.Background(), p)
	}
	return append([]byte(nil), t.Bytes()...), e
}

// Preflight applies independent size/count/depth bounds before generated Read
// methods allocate lists/maps. Protocol max bytes alone does not cap a declared
// container length. RPC envelopes use the same bounded scanner.
func scanThrift(ctx context.Context, format string, b []byte, rpc bool) (map[int16]thrift.TType, error) {
	if e := wireSize(b); e != nil {
		return nil, e
	}
	t := thrift.NewTMemoryBufferLen(len(b))
	t.Write(b)
	p := thriftProtocol(format, t)
	fields := 0
	seen := map[int16]thrift.TType{}
	var value func(thrift.TType, int, int) error
	value = func(kind thrift.TType, depth, role int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fields++
		if fields > 1024 || depth > 8 {
			return model.ErrBudget
		}
		switch kind {
		case thrift.STRUCT:
			if _, e := p.ReadStructBegin(ctx); e != nil {
				return e
			}
			local := map[int16]bool{}
			for {
				_, typ, id, e := p.ReadFieldBegin(ctx)
				if e != nil {
					return e
				}
				if typ == thrift.STOP {
					break
				}
				if local[id] {
					return model.ErrInvalid
				}
				local[id] = true
				if role == 0 {
					seen[id] = typ
				}
				child := 2
				if role == 0 && id == 4 && typ == thrift.LIST {
					child = 1
				}
				if role == 3 && (id == 0 || id == 1) && typ == thrift.STRUCT {
					child = 0
				}
				if role == 1 && id == 2 && (typ == thrift.I32 || typ == thrift.I64) {
					var n int64
					if typ == thrift.I32 {
						v, e := p.ReadI32(ctx)
						if e != nil {
							return e
						}
						n = int64(v)
					} else {
						v, e := p.ReadI64(ctx)
						if e != nil {
							return e
						}
						n = v
					}
					if n < 1 || n > 1000000000 {
						return model.ErrInvalid
					}
				} else if e = value(typ, depth+1, child); e != nil {
					return e
				}
				if e = p.ReadFieldEnd(ctx); e != nil {
					return e
				}
			}
			return p.ReadStructEnd(ctx)
		case thrift.STRING:
			s, e := p.ReadString(ctx)
			if e != nil {
				return e
			}
			if !model.Text(s, 256) {
				return model.ErrBudget
			}
		case thrift.BOOL:
			_, e := p.ReadBool(ctx)
			return e
		case thrift.BYTE:
			_, e := p.ReadByte(ctx)
			return e
		case thrift.I16:
			_, e := p.ReadI16(ctx)
			return e
		case thrift.I32:
			_, e := p.ReadI32(ctx)
			return e
		case thrift.I64:
			_, e := p.ReadI64(ctx)
			return e
		case thrift.DOUBLE:
			return model.ErrInvalid
		case thrift.LIST, thrift.SET:
			var typ thrift.TType
			var n int
			var e error
			if kind == thrift.LIST {
				typ, n, e = p.ReadListBegin(ctx)
			} else {
				typ, n, e = p.ReadSetBegin(ctx)
			}
			if e != nil {
				return e
			}
			if n < 0 || n > 100 {
				return model.ErrBudget
			}
			for i := 0; i < n; i++ {
				if e = value(typ, depth+1, role); e != nil {
					return e
				}
			}
			if kind == thrift.LIST {
				return p.ReadListEnd(ctx)
			}
			return p.ReadSetEnd(ctx)
		case thrift.MAP:
			k, v, n, e := p.ReadMapBegin(ctx)
			if e != nil {
				return e
			}
			if n < 0 || n > 32 {
				return model.ErrBudget
			}
			keys := map[string]bool{}
			for i := 0; i < n; i++ {
				if k == thrift.STRING {
					s, e := p.ReadString(ctx)
					if e != nil {
						return e
					}
					if !model.Text(s, 64) || keys[s] {
						return model.ErrInvalid
					}
					keys[s] = true
				} else if e = value(k, depth+1, 2); e != nil {
					return e
				}
				if e = value(v, depth+1, 2); e != nil {
					return e
				}
			}
			return p.ReadMapEnd(ctx)
		default:
			return model.ErrInvalid
		}
		return nil
	}
	role := 0
	if rpc {
		method, typ, _, e := p.ReadMessageBegin(ctx)
		if e != nil || method != "relay" || (typ != thrift.CALL && typ != thrift.REPLY) {
			return nil, model.ErrInvalid
		}
		role = 3
	}
	if e := value(thrift.STRUCT, 0, role); e != nil {
		return nil, e
	}
	if rpc {
		if e := p.ReadMessageEnd(ctx); e != nil {
			return nil, e
		}
	}
	if t.Len() != 0 {
		return nil, model.ErrInvalid
	}
	return seen, nil
}
func thriftDecode(ctx context.Context, format string, version int, b []byte) (model.Record, error) {
	var r model.Record
	seen, e := scanThrift(ctx, format, b, false)
	if e != nil {
		return r, e
	}
	t := thrift.NewTMemoryBufferLen(len(b))
	t.Write(b)
	p := thriftProtocol(format, t)
	if version == 1 {
		v := v1.NewSnapshot()
		if v.Read(ctx, p) != nil {
			return r, model.ErrInvalid
		}
		r = model.Record{Tenant: v.TenantID, ID: v.OrderID, Name: v.CustomerName, Version: v.Version, Expedited: v.Expedited, Lines: []model.Line{}}
		if v.LegacyNote != nil {
			r.LegacyNote = *v.LegacyNote
		}
		for _, l := range v.Lines {
			r.Lines = append(r.Lines, model.Line{SKU: l.Sku, Quantity: int64(l.Quantity)})
		}
	} else {
		v := v2.NewSnapshot()
		if v.Read(ctx, p) != nil {
			return r, model.ErrInvalid
		}
		r = model.Record{Tenant: v.TenantID, ID: v.OrderID, Name: v.DisplayName, Version: v.Version, Expedited: v.Expedited, Discount: v.DiscountMinor, Labels: v.Labels, Lines: []model.Line{}}
		if seen[6] == thrift.STRING {
			c := v.Currency
			r.Currency = &c
		}
		for _, l := range v.Lines {
			r.Lines = append(r.Lines, model.Line{SKU: l.Sku, Quantity: int64(l.Quantity)})
		}
	}
	return r, nil
}
