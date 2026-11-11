package encoding

import (
	"context"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

var Formats = []string{"json", "xml", "gob", "messagepack", "protobuf", "thrift-binary", "thrift-compact"}

func (s *Suite) Encode(format string, version int, r model.Record) ([]byte, error) {
	if version != 1 && version != 2 {
		return nil, model.ErrInvalid
	}
	if e := model.Validate(r); e != nil {
		return nil, e
	}
	var b []byte
	var e error
	switch format {
	case "json", "xml", "messagepack":
		b, e = textEncode(format, version, r)
	case "gob":
		b, e = gobEncode(version, r)
	case "protobuf":
		b, e = protoEncode(version, r)
	case "thrift-binary", "thrift-compact":
		b, e = thriftEncode(format, version, r)
	default:
		return nil, model.ErrInvalid
	}
	if e != nil {
		return nil, e
	}
	if e = wireSize(b); e != nil {
		return nil, e
	}
	return b, nil
}
func (s *Suite) Decode(ctx context.Context, format string, version int, b []byte) (model.Record, error) {
	var r model.Record
	if ctx.Err() != nil {
		return r, ctx.Err()
	}
	if version != 1 && version != 2 {
		return r, model.ErrInvalid
	}
	if e := wireSize(b); e != nil {
		return r, e
	}
	var e error
	switch format {
	case "json", "xml", "messagepack":
		r, e = textDecode(format, version, b)
	case "gob":
		r, e = s.gobDecode(ctx, version, b)
	case "protobuf":
		r, e = protoDecode(version, b)
	case "thrift-binary", "thrift-compact":
		r, e = thriftDecode(ctx, format, version, b)
	default:
		return r, model.ErrInvalid
	}
	if e != nil {
		return model.Record{}, e
	}
	if e = model.Validate(r); e != nil {
		return model.Record{}, e
	}
	return r, nil
}
