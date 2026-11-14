package objectstorage

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"sort"
)

const MaxGenerations = 128
const MaxSegments = 64
const MaxRecordsPerSegment = 128
const MaxLiveRows = 4096
const MaxChanges = 256

type Record struct {
	Key       string `json:"key"`
	Value     int64  `json:"value"`
	Tombstone bool   `json:"tombstone,omitempty"`
}
type Segment struct {
	Format  int      `json:"format"`
	Records []Record `json:"records"`
}
type Ref struct {
	Generation uint64 `json:"generation"`
	Key        string `json:"key"`
	SHA256     string `json:"sha256"`
}
type SegmentRef struct {
	Key     string `json:"key"`
	SHA256  string `json:"sha256"`
	Bytes   int    `json:"bytes"`
	Records int    `json:"records"`
}
type Receipt struct {
	ID         string `json:"id"`
	Hash       string `json:"request_sha256"`
	Generation uint64 `json:"generation"`
}
type Manifest struct {
	Format     int          `json:"format"`
	Generation uint64       `json:"generation"`
	Parent     Ref          `json:"parent"`
	Segments   []SegmentRef `json:"segments"`
	Receipts   []Receipt    `json:"receipts"`
}
type Head struct {
	Format  int `json:"format"`
	Current Ref `json:"current"`
}
type Command struct {
	ID      string   `json:"id"`
	Kind    string   `json:"kind"`
	Changes []Record `json:"changes"`
}
type Publication struct {
	Ref      Ref  `json:"snapshot"`
	Replayed bool `json:"replayed"`
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var intentPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
var rowKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}/[a-z][a-z0-9-]{0,31}/[A-Z0-9][A-Z0-9._-]{0,47}$`)

func strictJSON(b []byte, target any) error {
	if len(b) > 2*MaxObjectBytes {
		return ErrLimit
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return ErrCorrupt
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrCorrupt
	}
	return nil
}
func validRecord(r Record) bool {
	return rowKeyPattern.MatchString(r.Key) && r.Value >= 0 && r.Value <= 1000000000 && (!r.Tombstone || r.Value == 0)
}
func canonicalCommand(c Command) (Command, string, error) {
	if !intentPattern.MatchString(c.ID) || (c.Kind != "update" && c.Kind != "compact") || (c.Kind == "update" && (len(c.Changes) == 0 || len(c.Changes) > MaxChanges)) || (c.Kind == "compact" && len(c.Changes) != 0) {
		return c, "", ErrInvalid
	}
	c.Changes = append([]Record(nil), c.Changes...)
	sort.Slice(c.Changes, func(i, j int) bool { return c.Changes[i].Key < c.Changes[j].Key })
	for i, r := range c.Changes {
		if !validRecord(r) || (i > 0 && c.Changes[i-1].Key == r.Key) {
			return c, "", ErrInvalid
		}
	}
	b, _ := json.Marshal(c)
	return c, sum(b), nil
}
func validRef(r Ref) bool {
	return r.Generation > 0 && r.Generation <= MaxGenerations && digestPattern.MatchString(r.SHA256) && r.Key == "manifests/"+r.SHA256+".json"
}
func validSegmentRef(r SegmentRef) bool {
	return digestPattern.MatchString(r.SHA256) && r.Key == "segments/"+r.SHA256+".json" && r.Bytes > 0 && r.Bytes <= MaxObjectBytes && r.Records > 0 && r.Records <= MaxRecordsPerSegment
}
func decodeManifest(b []byte, ref Ref) (Manifest, error) {
	var m Manifest
	if !validRef(ref) || sum(b) != ref.SHA256 || strictJSON(b, &m) != nil || m.Format != 1 || m.Generation != ref.Generation || len(m.Segments) > MaxSegments || len(m.Receipts) != int(m.Generation) {
		return m, ErrCorrupt
	}
	if m.Generation == 1 {
		if m.Parent != (Ref{}) {
			return m, ErrCorrupt
		}
	} else if !validRef(m.Parent) || m.Parent.Generation != m.Generation-1 {
		return m, ErrCorrupt
	}
	ids := map[string]bool{}
	for i, r := range m.Receipts {
		if !intentPattern.MatchString(r.ID) || !digestPattern.MatchString(r.Hash) || r.Generation != uint64(i+1) || ids[r.ID] {
			return m, ErrCorrupt
		}
		ids[r.ID] = true
	}
	for _, r := range m.Segments {
		if !validSegmentRef(r) {
			return m, ErrCorrupt
		}
	}
	return m, nil
}
func decodeSegment(b []byte, ref SegmentRef) ([]Record, error) {
	var s Segment
	if !validSegmentRef(ref) || len(b) != ref.Bytes || sum(b) != ref.SHA256 || strictJSON(b, &s) != nil || s.Format != 1 || len(s.Records) != ref.Records {
		return nil, ErrCorrupt
	}
	for i, r := range s.Records {
		if !validRecord(r) || (i > 0 && s.Records[i-1].Key >= r.Key) {
			return nil, ErrCorrupt
		}
	}
	return s.Records, nil
}
