// Package avrolab exercises actual Avro writer/reader resolution, immutable
// schema lookup and archived synthetic order snapshots.
package avrolab

import (
	"crypto/sha256"
	"embed"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"regexp"
	"sync"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/avroprofile"
	"github.com/twmb/avro"
)

//go:embed generated/schema/*.avsc
var schemaFiles embed.FS

var ErrMissingSchema = errors.New("writer schema is unavailable")
var ErrImmutable = errors.New("immutable identity already names different content")
var ErrAmbiguous = errors.New("fingerprint names more than one full schema")
var ErrIncompatible = errors.New("Avro writer and reader schemas are incompatible")
var versionName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,47}$`)

type SchemaInfo struct {
	Version         string `json:"version"`
	SHA256          string `json:"full_schema_sha256"`
	CanonicalSHA256 string `json:"parsing_canonical_sha256"`
	Rabin           string `json:"crc64_avro"`
}
type schemaEntry struct {
	info        SchemaInfo
	full        []byte
	schema      *avro.Schema
	plan        *avroprofile.Node
	fingerprint uint64
}
type Registry struct {
	mu           sync.RWMutex
	versions     map[string]*schemaEntry
	digests      map[string]*schemaEntry
	fingerprints map[uint64]map[string]*schemaEntry
}

func NewRegistry() *Registry {
	return &Registry{versions: map[string]*schemaEntry{}, digests: map[string]*schemaEntry{}, fingerprints: map[uint64]map[string]*schemaEntry{}}
}
func entry(version string, raw []byte) (*schemaEntry, error) {
	if !versionName.MatchString(version) {
		return nil, avroprofile.ErrInvalid
	}
	s, plan, full, e := avroprofile.Compile(raw)
	if e != nil {
		return nil, e
	}
	digest := sha256.Sum256(full)
	canonical := sha256.Sum256(s.Canonical())
	h := avro.NewRabin()
	h.Write(s.Canonical())
	fp := h.Sum64()
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], fp)
	return &schemaEntry{SchemaInfo{version, hex.EncodeToString(digest[:]), hex.EncodeToString(canonical[:]), hex.EncodeToString(encoded[:])}, full, s, plan, fp}, nil
}

// Register is append-only. Full-schema identity includes reader defaults and
// aliases; a CRC collision or canonical-equivalent contract stays ambiguous.
func (r *Registry) Register(version string, raw []byte) (SchemaInfo, error) {
	v, e := entry(version, raw)
	if e != nil {
		return SchemaInfo{}, e
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old := r.versions[version]; old != nil {
		if old.info.SHA256 != v.info.SHA256 {
			return SchemaInfo{}, ErrImmutable
		}
		return old.info, nil
	}
	if len(r.versions) >= 32 {
		return SchemaInfo{}, avroprofile.ErrBudget
	}
	r.versions[version] = v
	if r.digests[v.info.SHA256] == nil {
		r.digests[v.info.SHA256] = v
	}
	if r.fingerprints[v.fingerprint] == nil {
		r.fingerprints[v.fingerprint] = map[string]*schemaEntry{}
	}
	r.fingerprints[v.fingerprint][v.info.SHA256] = v
	return v.info, nil
}
func (r *Registry) byVersion(version string) (*schemaEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v := r.versions[version]
	if v == nil {
		return nil, ErrMissingSchema
	}
	return v, nil
}
func (r *Registry) byDigest(digest string) (*schemaEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v := r.digests[digest]
	if v == nil {
		return nil, ErrMissingSchema
	}
	return v, nil
}
func (r *Registry) byFingerprint(fp uint64) (*schemaEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := r.fingerprints[fp]
	if len(versions) == 0 {
		return nil, ErrMissingSchema
	}
	if len(versions) != 1 {
		return nil, ErrAmbiguous
	}
	for _, v := range versions {
		return v, nil
	}
	return nil, ErrMissingSchema
}
func (r *Registry) Info(version string) (SchemaInfo, error) {
	v, e := r.byVersion(version)
	if e != nil {
		return SchemaInfo{}, e
	}
	return v.info, nil
}
func (r *Registry) Schema(version string) ([]byte, error) {
	v, e := r.byVersion(version)
	if e != nil {
		return nil, e
	}
	return append([]byte(nil), v.full...), nil
}
func BuiltinRegistry() (*Registry, error) {
	r := NewRegistry()
	for _, version := range []string{"v1", "v2", "v3"} {
		b, e := schemaFiles.ReadFile("generated/schema/" + version + ".avsc")
		if e != nil {
			return nil, e
		}
		if _, e = r.Register(version, b); e != nil {
			return nil, e
		}
	}
	return r, nil
}
