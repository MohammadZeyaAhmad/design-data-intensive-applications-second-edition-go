package catalog

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
)

type BrandSourceRecord struct {
	Tenant         string    `json:"tenant"`
	ID             string    `json:"id"`
	Version        int64     `json:"version"`
	SchemaVersion  int       `json:"schema_version"`
	SourceSHA256   string    `json:"source_sha256"`
	Body           []byte    `json:"body_base64"`
	Metadata       []byte    `json:"metadata_base64"`
	MetadataSHA256 string    `json:"metadata_sha256"`
	RecordedAt     time.Time `json:"recorded_at"`
}
type BrandWriterSchema struct {
	Version    int    `json:"version"`
	SHA256     string `json:"sha256"`
	Definition []byte `json:"definition_base64"`
}
type BrandHistoryArchive struct {
	FormatVersion int                 `json:"archive_format_version"`
	Schemas       []BrandWriterSchema `json:"writer_schemas"`
	Records       []BrandSourceRecord `json:"records"`
}

const MaxBrandArchive = 1 << 20

func EncodeBrandArchive(records []BrandSourceRecord) ([]byte, error) {
	if len(records) < 1 || len(records) > 256 {
		return nil, ErrHistorySchema
	}
	a := BrandHistoryArchive{FormatVersion: 1, Records: records}
	for _, version := range []int{1, 2} {
		raw, _ := BrandHistorySchema(version)
		a.Schemas = append(a.Schemas, BrandWriterSchema{version, HistoryDigest(raw), raw})
	}
	for _, record := range records {
		snapshot, e := UpcastBrandHistory(record.SchemaVersion, record.Body)
		if e != nil || snapshot.ID != record.ID || snapshot.Version != record.Version || len(record.Tenant) < 1 || len(record.Tenant) > 64 || HistoryDigest(record.Body) != record.SourceSHA256 || len(record.Metadata) > 4096 || !json.Valid(record.Metadata) || HistoryDigest(record.Metadata) != record.MetadataSHA256 || record.RecordedAt.IsZero() {
			return nil, ErrHistorySchema
		}
	}
	encoded, e := json.MarshalIndent(a, "", "  ")
	if e != nil || len(encoded) > MaxBrandArchive {
		return nil, ErrHistorySchema
	}
	return append(encoded, '\n'), nil
}
func ReplayBrandArchive(raw []byte) ([]BrandSnapshot, error) {
	if len(raw) > MaxBrandArchive {
		return nil, ErrHistorySchema
	}
	if e := scanBrandArchive(raw); e != nil {
		return nil, e
	}
	var a BrandHistoryArchive
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&a) != nil {
		return nil, ErrHistorySchema
	}
	if _, e := d.Token(); e != io.EOF {
		return nil, ErrHistorySchema
	}
	if a.FormatVersion != 1 || len(a.Records) < 1 || len(a.Records) > 256 || len(a.Schemas) != 2 {
		return nil, ErrHistorySchema
	}
	seen := map[int]bool{}
	for _, schema := range a.Schemas {
		known, e := BrandHistorySchema(schema.Version)
		if e != nil || seen[schema.Version] || !bytes.Equal(known, schema.Definition) || HistoryDigest(schema.Definition) != schema.SHA256 {
			return nil, ErrHistorySchema
		}
		seen[schema.Version] = true
	}
	var out []BrandSnapshot
	for _, record := range a.Records {
		if !seen[record.SchemaVersion] || len(record.Tenant) < 1 || len(record.Tenant) > 64 || HistoryDigest(record.Body) != record.SourceSHA256 || len(record.Metadata) > 4096 || !json.Valid(record.Metadata) || HistoryDigest(record.Metadata) != record.MetadataSHA256 || record.RecordedAt.IsZero() {
			return nil, ErrHistorySchema
		}
		snapshot, e := UpcastBrandHistory(record.SchemaVersion, record.Body)
		if e != nil || snapshot.ID != record.ID || snapshot.Version != record.Version {
			return nil, ErrHistorySchema
		}
		out = append(out, snapshot)
	}
	return out, nil
}

// Bound counts/depth and reject duplicate keys before decoding typed slices.
// Source bodies/metadata are base64 images, so their own contracts are checked
// separately after this finite archive envelope has been parsed.
func scanBrandArchive(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	tokens := 0
	var value func(int) error
	value = func(depth int) error {
		tokens++
		if depth > 12 || tokens > 12000 {
			return ErrHistorySchema
		}
		token, e := d.Token()
		if e != nil {
			return ErrHistorySchema
		}
		switch v := token.(type) {
		case string:
			if len(v) > 8192 {
				return ErrHistorySchema
			}
		case json.Delim:
			switch v {
			case '{':
				keys := map[string]bool{}
				for d.More() {
					key, e := d.Token()
					if e != nil {
						return ErrHistorySchema
					}
					name, ok := key.(string)
					if !ok || len(name) > 64 || keys[name] || len(keys) >= 16 {
						return ErrHistorySchema
					}
					keys[name] = true
					if e = value(depth + 1); e != nil {
						return e
					}
				}
				last, e := d.Token()
				if e != nil || last != json.Delim('}') {
					return ErrHistorySchema
				}
			case '[':
				count := 0
				for d.More() {
					count++
					if count > 256 {
						return ErrHistorySchema
					}
					if e = value(depth + 1); e != nil {
						return e
					}
				}
				last, e := d.Token()
				if e != nil || last != json.Delim(']') {
					return ErrHistorySchema
				}
			default:
				return ErrHistorySchema
			}
		}
		return nil
	}
	if e := value(0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return ErrHistorySchema
	}
	return nil
}
