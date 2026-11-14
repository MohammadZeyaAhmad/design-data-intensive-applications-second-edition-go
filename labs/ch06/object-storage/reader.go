package objectstorage

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
)

type Reader struct {
	Store Store
	Cache *Cache
}
type Snapshot struct {
	Ref  Ref      `json:"ref"`
	Rows []Record `json:"rows"`
}

func (r Reader) Head(ctx context.Context) (Head, string, error) {
	o, err := r.Store.Get(ctx, "head.json")
	if err != nil {
		return Head{}, "", err
	}
	var h Head
	if strictJSON(o.Body, &h) != nil || h.Format != 1 || !validRef(h.Current) || o.Version == "" {
		return h, "", ErrCorrupt
	}
	return h, o.Version, nil
}
func (r Reader) manifest(ctx context.Context, ref Ref) (Manifest, error) {
	if !validRef(ref) {
		return Manifest{}, ErrCorrupt
	}
	b, err := r.Cache.fetch(ctx, r.Store, ref.Key, ref.SHA256)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			err = ErrCorrupt
		}
		return Manifest{}, err
	}
	return decodeManifest(b, ref)
}
func (r Reader) Read(ctx context.Context, ref Ref) (Snapshot, error) {
	m, err := r.manifest(ctx, ref)
	if err != nil {
		return Snapshot{}, err
	}
	state := map[string]Record{}
	for _, segment := range m.Segments {
		b, e := r.Cache.fetch(ctx, r.Store, segment.Key, segment.SHA256)
		if e != nil {
			if errors.Is(e, ErrNotFound) {
				e = ErrCorrupt
			}
			return Snapshot{}, e
		}
		records, e := decodeSegment(b, segment)
		if e != nil {
			return Snapshot{}, e
		}
		for _, record := range records {
			if record.Tombstone {
				delete(state, record.Key)
			} else {
				state[record.Key] = record
			}
			if len(state) > MaxLiveRows {
				return Snapshot{}, ErrLimit
			}
		}
	}
	rows := make([]Record, 0, len(state))
	for _, record := range state {
		rows = append(rows, record)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return Snapshot{ref, rows}, nil
}
func (r Reader) Current(ctx context.Context) (Snapshot, error) {
	h, _, err := r.Head(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return r.Read(ctx, h.Current)
}
func (r Reader) Pin(ctx context.Context, name string, ref Ref) error {
	if !intentPattern.MatchString(name) || !validRef(ref) {
		return ErrInvalid
	}
	// A complete orphan is still unpublished. Named snapshots must come from
	// the published lineage, rather than establish another publication path.
	head, _, err := r.Head(ctx)
	if err != nil {
		return err
	}
	current := head.Current
	for current.Generation > ref.Generation {
		m, e := r.manifest(ctx, current)
		if e != nil {
			return e
		}
		current = m.Parent
	}
	if current != ref {
		return ErrConflict
	}
	if _, err := r.Read(ctx, ref); err != nil {
		return err
	}
	b, _ := json.Marshal(Head{1, ref})
	_, err = r.Store.Put(ctx, "pins/"+name+".json", b, Condition{Absent: true})
	if err != nil {
		old, e := r.Store.Get(ctx, "pins/"+name+".json")
		if e == nil && string(old.Body) == string(b) {
			return nil
		}
	}
	return err
}
func (r Reader) Pinned(ctx context.Context, name string) (Snapshot, error) {
	if !intentPattern.MatchString(name) {
		return Snapshot{}, ErrInvalid
	}
	o, err := r.Store.Get(ctx, "pins/"+name+".json")
	if err != nil {
		return Snapshot{}, err
	}
	var h Head
	if strictJSON(o.Body, &h) != nil || h.Format != 1 || !validRef(h.Current) {
		return Snapshot{}, ErrCorrupt
	}
	return r.Read(ctx, h.Current)
}
