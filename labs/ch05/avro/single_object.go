package avrolab

import (
	"context"
	"encoding/binary"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

func (r *Registry) EncodeSingleObject(ctx context.Context, version, mode string, record model.Record) ([]byte, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	writer, e := r.byVersion(version)
	if e != nil {
		return nil, e
	}
	value, e := input(mode, version, record)
	if e != nil {
		return nil, e
	}
	wire, e := writer.schema.AppendSingleObject(nil, value)
	if e != nil {
		return nil, avroprofile.ErrInvalid
	}
	if len(wire) > avroprofile.MaxWire+10 {
		return nil, avroprofile.ErrBudget
	}
	if e = avroprofile.Scan(writer.plan, wire[10:]); e != nil {
		return nil, e
	}
	return wire, nil
}
func (r *Registry) DecodeSingleObject(ctx context.Context, readerVersion, mode string, wire []byte) (model.Record, error) {
	if len(wire) > avroprofile.MaxWire+10 {
		return model.Record{}, avroprofile.ErrBudget
	}
	fingerprint, payload, e := avro.SingleObjectFingerprint(wire)
	if e != nil {
		return model.Record{}, avroprofile.ErrInvalid
	}
	writer, e := r.byFingerprint(binary.LittleEndian.Uint64(fingerprint[:]))
	if e != nil {
		return model.Record{}, e
	}
	reader, e := r.byVersion(readerVersion)
	if e != nil {
		return model.Record{}, e
	}
	return decode(ctx, writer, reader, readerVersion, mode, payload)
}

// DecodePinnedSingleObject uses caller-supplied immutable schema metadata when
// a CRC alone is ambiguous. The header must still match that writer's layout.
func (r *Registry) DecodePinnedSingleObject(ctx context.Context, writerDigest, readerVersion, mode string, wire []byte) (model.Record, error) {
	if len(wire) > avroprofile.MaxWire+10 {
		return model.Record{}, avroprofile.ErrBudget
	}
	fingerprint, payload, e := avro.SingleObjectFingerprint(wire)
	if e != nil {
		return model.Record{}, avroprofile.ErrInvalid
	}
	writer, e := r.byDigest(writerDigest)
	if e != nil {
		return model.Record{}, e
	}
	if binary.LittleEndian.Uint64(fingerprint[:]) != writer.fingerprint {
		return model.Record{}, avroprofile.ErrInvalid
	}
	reader, e := r.byVersion(readerVersion)
	if e != nil {
		return model.Record{}, e
	}
	return decode(ctx, writer, reader, readerVersion, mode, payload)
}
