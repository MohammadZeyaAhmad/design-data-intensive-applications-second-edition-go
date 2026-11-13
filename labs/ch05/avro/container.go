package avrolab

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro/ocf"
)

const MaxArchive = 1 << 20
const MaxRecords = 128

type ArchiveInfo struct {
	Writer       SchemaInfo `json:"writer"`
	FileSHA256   string     `json:"file_sha256"`
	Records      int        `json:"records"`
	Codec        string     `json:"codec"`
	EncodedBytes int        `json:"encoded_bytes"`
}

func hashBytes(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// inspectContainer bounds header metadata, schema compilation, compressed and
// total decompressed bytes, block counts and every writer-schema datum before
// the native OCF decoder runs. It accepts null/deflate in this finite profile.
func inspectContainer(ctx context.Context, wire []byte) (ArchiveInfo, *schemaEntry, error) {
	var info ArchiveInfo
	if len(wire) > MaxArchive {
		return info, nil, avroprofile.ErrBudget
	}
	if len(wire) < 4 || !bytes.Equal(wire[:4], []byte{'O', 'b', 'j', 1}) {
		return info, nil, avroprofile.ErrInvalid
	}
	c := avroprofile.Cursor{Data: wire, Pos: 4}
	metadata := map[string][]byte{}
	total := int64(0)
	for {
		count, e := c.Long()
		if e != nil {
			return info, nil, e
		}
		if count == 0 {
			break
		}
		end := -1
		if count < 0 {
			if count == math.MinInt64 {
				return info, nil, avroprofile.ErrBudget
			}
			count = -count
			size, e := c.Long()
			if e != nil || size < 0 || size > int64(len(wire)-c.Pos) {
				return info, nil, avroprofile.ErrInvalid
			}
			end = c.Pos + int(size)
		}
		if count > 8-total {
			return info, nil, avroprofile.ErrBudget
		}
		total += count
		for i := int64(0); i < count; i++ {
			key, e := c.String(64)
			if e != nil {
				return info, nil, e
			}
			if _, ok := metadata[key]; ok {
				return info, nil, avroprofile.ErrInvalid
			}
			value, e := c.Bytes(avroprofile.MaxSchema)
			if e != nil {
				return info, nil, e
			}
			metadata[key] = value
		}
		if end >= 0 && end != c.Pos {
			return info, nil, avroprofile.ErrInvalid
		}
	}
	marker, e := c.Take(16)
	if e != nil {
		return info, nil, e
	}
	writer, e := entry(string(metadata["atlas.schema.version"]), metadata["avro.schema"])
	if e != nil {
		return info, nil, e
	}
	if string(metadata["atlas.schema.sha256"]) != writer.info.SHA256 {
		return info, nil, avroprofile.ErrInvalid
	}
	codec := "null"
	if value, ok := metadata["avro.codec"]; ok {
		codec = string(value)
	}
	if codec != "null" && codec != "deflate" {
		return info, nil, avroprofile.ErrInvalid
	}
	countRecords, totalInflated := 0, 0
	for c.Pos < len(wire) {
		if e = ctx.Err(); e != nil {
			return info, nil, e
		}
		count, e := c.Long()
		if e != nil || count < 1 || count > int64(MaxRecords-countRecords) {
			return info, nil, avroprofile.ErrBudget
		}
		size, e := c.Long()
		if e != nil || size < 0 || size > avroprofile.MaxWire {
			return info, nil, avroprofile.ErrBudget
		}
		block, e := c.Take(int(size))
		if e != nil {
			return info, nil, e
		}
		if codec == "deflate" {
			source := bytes.NewReader(block)
			reader := flate.NewReader(source)
			decoded, readError := io.ReadAll(io.LimitReader(reader, avroprofile.MaxWire+1))
			closeError := reader.Close()
			if readError != nil || closeError != nil || source.Len() != 0 {
				return info, nil, avroprofile.ErrInvalid
			}
			if len(decoded) > avroprofile.MaxWire {
				return info, nil, avroprofile.ErrBudget
			}
			block = decoded
		}
		totalInflated += len(block)
		if totalInflated > MaxArchive {
			return info, nil, avroprofile.ErrBudget
		}
		datum := avroprofile.Cursor{Data: block}
		for i := int64(0); i < count; i++ {
			datum.Values = 0
			if e = datum.Value(writer.plan, 0); e != nil {
				return info, nil, e
			}
		}
		if datum.Pos != len(block) {
			return info, nil, avroprofile.ErrInvalid
		}
		sync, e := c.Take(16)
		if e != nil || !bytes.Equal(sync, marker) {
			return info, nil, avroprofile.ErrInvalid
		}
		countRecords += int(count)
	}
	if countRecords < 1 {
		return info, nil, avroprofile.ErrInvalid
	}
	info = ArchiveInfo{writer.info, hashBytes(wire), countRecords, codec, len(wire)}
	return info, writer, nil
}

// BuildContainer writes a standard OCF with the complete immutable writer
// schema in its header, plus semantic identity metadata. Defaults stay intact.
func (r *Registry) BuildContainer(ctx context.Context, version, mode, codec string, records []model.Record) ([]byte, ArchiveInfo, error) {
	var info ArchiveInfo
	if len(records) < 1 || len(records) > MaxRecords {
		return nil, info, avroprofile.ErrBudget
	}
	writer, e := r.byVersion(version)
	if e != nil {
		return nil, info, e
	}
	var compression ocf.Codec
	switch codec {
	case "null":
	case "deflate":
		compression = ocf.DeflateCodec(flate.DefaultCompression)
	default:
		return nil, info, avroprofile.ErrInvalid
	}
	var buffer bytes.Buffer
	opts := []ocf.WriterOpt{ocf.WithSchema(string(writer.full)), ocf.WithBlockCount(16), ocf.WithBlockBytes(32 << 10), ocf.WithMetadata(map[string][]byte{"atlas.schema.sha256": []byte(writer.info.SHA256), "atlas.schema.version": []byte(version)})}
	if compression != nil {
		opts = append(opts, ocf.WithCodec(compression))
	}
	w, e := ocf.NewWriter(&buffer, writer.schema, opts...)
	if e != nil {
		return nil, info, e
	}
	for _, record := range records {
		if e = ctx.Err(); e != nil {
			w.Close()
			return nil, info, e
		}
		v, inputError := input(mode, version, record)
		if inputError != nil {
			w.Close()
			return nil, info, inputError
		}
		if e = w.Encode(v); e != nil {
			w.Close()
			return nil, info, e
		}
		if buffer.Len() > MaxArchive {
			w.Close()
			return nil, info, avroprofile.ErrBudget
		}
	}
	if e = w.Close(); e != nil {
		return nil, info, e
	}
	wire := append([]byte(nil), buffer.Bytes()...)
	info, _, e = inspectContainer(ctx, wire)
	if e != nil {
		return nil, ArchiveInfo{}, e
	}
	return wire, info, nil
}

// ReplayContainer needs only the chosen reader schema in the registry. Its
// actual writer schema comes from the archived header, never a latest alias.
func (r *Registry) ReplayContainer(ctx context.Context, readerVersion string, wire []byte) ([]model.Record, ArchiveInfo, error) {
	info, _, e := inspectContainer(ctx, wire)
	if e != nil {
		return nil, ArchiveInfo{}, e
	}
	reader, e := r.byVersion(readerVersion)
	if e != nil {
		return nil, ArchiveInfo{}, e
	}
	native, e := ocf.NewReader(bytes.NewReader(wire), ocf.WithReaderSchema(reader.schema), ocf.WithMaxBlockBytes(avroprofile.MaxWire), ocf.WithMaxDecompressedBlockBytes(avroprofile.MaxWire))
	if e != nil {
		return nil, ArchiveInfo{}, e
	}
	defer native.Close()
	var records []model.Record
	for {
		if e = ctx.Err(); e != nil {
			return nil, ArchiveInfo{}, e
		}
		var value any
		e = native.Decode(&value)
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return nil, ArchiveInfo{}, e
		}
		record, e := recordFromMap(readerVersion, value)
		if e != nil {
			return nil, ArchiveInfo{}, e
		}
		if e = model.Validate(record); e != nil {
			return nil, ArchiveInfo{}, e
		}
		records = append(records, record)
		if len(records) > MaxRecords {
			return nil, ArchiveInfo{}, avroprofile.ErrBudget
		}
	}
	if len(records) != info.Records {
		return nil, ArchiveInfo{}, avroprofile.ErrInvalid
	}
	return records, info, nil
}
