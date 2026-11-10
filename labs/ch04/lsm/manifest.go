package lsm

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sort"
)

type manifest struct {
	Version     int          `json:"version"`
	Generation  uint64       `json:"generation"`
	Checkpoint  uint64       `json:"checkpoint"`
	NextID      uint64       `json:"next_id"`
	Style       Style        `json:"style"`
	Initialized bool         `json:"initialized"`
	Tables      []descriptor `json:"tables"`
}
type manifestEnvelope struct {
	Data   json.RawMessage `json:"data"`
	SHA256 string          `json:"sha256"`
}

func validateManifest(m manifest) error {
	if m.Version != 1 || m.Generation == 0 || m.NextID == 0 || (m.Style != Leveled && m.Style != Tiered) || len(m.Tables) > maxTables {
		return corruption("manifest version or bounds")
	}
	if !m.Initialized && (m.Checkpoint != 0 || len(m.Tables) != 0 || m.NextID != 1) {
		return corruption("unfinished initialization contains data")
	}
	seen := map[uint64]bool{}
	for _, t := range m.Tables {
		if t.ID == 0 || t.ID >= m.NextID || seen[t.ID] || t.Level < 0 || t.Level > maxLevel || t.Count < 1 || t.Count > maxEntries || t.Size < 32 || t.Size > maxFileBytes || len(t.SHA256) != 64 || !validKey(t.Min) || !validKey(t.Max) || bytes.Compare(t.Min, t.Max) > 0 || t.MinSeq == 0 || t.MaxSeq < t.MinSeq || t.MaxSeq > m.Checkpoint {
			return corruption("table descriptor")
		}
		if _, e := hex.DecodeString(t.SHA256); e != nil {
			return corruption("table hash encoding")
		}
		seen[t.ID] = true
	}
	if m.Style == Leveled {
		for level := 1; level <= maxLevel; level++ {
			var ts []descriptor
			for _, t := range m.Tables {
				if t.Level == level {
					ts = append(ts, t)
				}
			}
			sort.Slice(ts, func(i, j int) bool { return bytes.Compare(ts[i].Min, ts[j].Min) < 0 })
			for i := 1; i < len(ts); i++ {
				if bytes.Compare(ts[i-1].Max, ts[i].Min) >= 0 {
					return corruption("overlapping leveled ranges")
				}
			}
		}
	}
	return nil
}
func (d *DB) readManifest() (manifest, error) {
	var m manifest
	f, e := d.fs.Open(d.path("MANIFEST"), os.O_RDONLY)
	if e != nil {
		return m, e
	}
	defer f.Close()
	s, e := f.Size()
	if e != nil {
		return m, e
	}
	if s < 1 || s > 1<<20 {
		return m, corruption("manifest size")
	}
	b := make([]byte, s)
	if _, e = f.ReadAt(b, 0); e != nil {
		return m, e
	}
	var envelope manifestEnvelope
	if e = strictJSON(b, &envelope); e != nil {
		return m, corruption("manifest envelope")
	}
	h := sha256.Sum256(envelope.Data)
	if hex.EncodeToString(h[:]) != envelope.SHA256 {
		return m, corruption("manifest checksum")
	}
	if e = strictJSON(envelope.Data, &m); e != nil {
		return m, corruption("manifest encoding")
	}
	return m, validateManifest(m)
}
func (d *DB) writeManifest(m manifest, op string) error {
	if e := validateManifest(m); e != nil {
		return e
	}
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	h := sha256.Sum256(b)
	b, e = json.Marshal(manifestEnvelope{b, hex.EncodeToString(h[:])})
	if e != nil {
		return e
	}
	if len(b) > 1<<20 {
		return ErrLimit
	}
	f, e := d.fs.Open(d.path("MANIFEST.tmp"), os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	if e != nil {
		return e
	}
	defer f.Close()
	n, e := writeAll(f, b)
	d.count.ManifestBytes += uint64(n)
	if e != nil {
		return e
	}
	if e = d.hit(op + ".manifest.written"); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	d.count.Syncs++
	if e = d.hit(op + ".manifest.synced"); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = d.fs.Rename(d.path("MANIFEST.tmp"), d.path("MANIFEST")); e != nil {
		return e
	}
	if e = d.hit(op + ".manifest.renamed"); e != nil {
		return e
	}
	if e = d.syncDir(); e != nil {
		return e
	}
	return d.hit(op + ".manifest.dir_synced")
}

func (d *DB) publish(m manifest, op string) error {
	if m.Generation == ^uint64(0) {
		return ErrLimit
	}
	m.Generation++
	m.NextID = d.nextID
	// This sync MUST precede publication: a persisted manifest must never name
	// a table whose rename could disappear in a power failure.
	if e := d.syncDir(); e != nil {
		return e
	}
	if e := d.hit(op + ".table.dir_synced"); e != nil {
		return e
	}
	next := make(map[uint64]*table, len(m.Tables))
	var opened []*table
	committed := false
	defer func() {
		if !committed {
			for _, t := range opened {
				t.file.Close()
			}
		}
	}()
	for _, desc := range m.Tables {
		if t := d.tables[desc.ID]; t != nil {
			next[desc.ID] = t
			continue
		}
		t, e := d.openTable(desc)
		if e != nil {
			return e
		}
		next[desc.ID] = t
		opened = append(opened, t)
	}
	if e := d.writeManifest(m, op); e != nil {
		return e
	}
	old := d.tables
	d.tables = next
	d.manifest = m
	committed = true
	var closeErrors []error
	for id, t := range old {
		if next[id] == nil {
			closeErrors = append(closeErrors, t.file.Close())
		}
	}
	if e := errors.Join(closeErrors...); e != nil {
		return e
	}
	for id := range old {
		if next[id] == nil {
			if e := d.fs.Remove(d.path(tableName(id))); e != nil {
				return e
			}
			if e := d.hit(op + ".obsolete.removed"); e != nil {
				return e
			}
		}
	}
	return d.syncDir()
}
