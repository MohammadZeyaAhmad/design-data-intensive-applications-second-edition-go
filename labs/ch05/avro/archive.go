package avrolab

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
)

type Manifest struct {
	Format  int    `json:"manifest_version"`
	Dataset string `json:"dataset"`
	ArchiveInfo
}

// publish installs a fully synced inode with atomic create-if-absent linking.
// An identical retry is allowed; replacement is never allowed. A failed manifest
// publication can leave an unreachable content object, without a partial read.
func publish(path string, b []byte) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".avro-publish-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Link(f.Name(), path); e != nil {
		if !errors.Is(e, os.ErrExist) {
			return e
		}
		old, e := readBounded(path, MaxArchive)
		if e != nil {
			return e
		}
		if !bytes.Equal(old, b) {
			return ErrImmutable
		}
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func readBounded(path string, max int) ([]byte, error) {
	stat, e := os.Lstat(path)
	if e != nil {
		return nil, e
	}
	if !stat.Mode().IsRegular() || stat.Size() > int64(max) {
		return nil, avroprofile.ErrBudget
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if e != nil {
		return nil, e
	}
	if len(b) > max {
		return nil, avroprofile.ErrBudget
	}
	return b, nil
}

// PublishDataset adds one immutable, named dataset to a caller-owned private
// directory. Content-addressed OCF bytes retain their embedded writer schema.
func PublishDataset(ctx context.Context, root, dataset string, wire []byte) (Manifest, error) {
	var manifest Manifest
	if !versionName.MatchString(dataset) {
		return manifest, avroprofile.ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return manifest, e
	}
	info, writer, e := inspectContainer(ctx, wire)
	if e != nil {
		return manifest, e
	}
	// Do not publish structurally valid bytes that fail the snapshot domain.
	check := NewRegistry()
	if _, e = check.Register(writer.info.Version, writer.full); e != nil {
		return manifest, e
	}
	if _, _, e = check.ReplayContainer(ctx, writer.info.Version, wire); e != nil {
		return manifest, e
	}
	manifest = Manifest{1, dataset, info}
	if e = publish(filepath.Join(root, "objects", info.FileSHA256+".avro"), wire); e != nil {
		return Manifest{}, e
	}
	b, e := json.MarshalIndent(manifest, "", "  ")
	if e != nil {
		return Manifest{}, e
	}
	b = append(b, '\n')
	if e = publish(filepath.Join(root, "datasets", dataset+".json"), b); e != nil {
		return Manifest{}, e
	}
	return manifest, nil
}
func (r *Registry) ReplayDataset(ctx context.Context, root, dataset, readerVersion string) ([]model.Record, Manifest, error) {
	var manifest Manifest
	if !versionName.MatchString(dataset) {
		return nil, manifest, avroprofile.ErrInvalid
	}
	b, e := readBounded(filepath.Join(root, "datasets", dataset+".json"), 4096)
	if e != nil {
		return nil, manifest, e
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&manifest) != nil {
		return nil, Manifest{}, avroprofile.ErrInvalid
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, Manifest{}, avroprofile.ErrInvalid
	}
	hash, e := hex.DecodeString(manifest.FileSHA256)
	if e != nil || len(hash) != 32 || manifest.Format != 1 || manifest.Dataset != dataset || manifest.Records < 1 || manifest.Records > MaxRecords {
		return nil, Manifest{}, avroprofile.ErrInvalid
	}
	wire, e := readBounded(filepath.Join(root, "objects", manifest.FileSHA256+".avro"), MaxArchive)
	if e != nil {
		return nil, Manifest{}, e
	}
	if hashBytes(wire) != manifest.FileSHA256 {
		return nil, Manifest{}, avroprofile.ErrInvalid
	}
	records, info, e := r.ReplayContainer(ctx, readerVersion, wire)
	if e != nil {
		return nil, Manifest{}, e
	}
	if info != manifest.ArchiveInfo {
		return nil, Manifest{}, avroprofile.ErrInvalid
	}
	return records, manifest, nil
}
