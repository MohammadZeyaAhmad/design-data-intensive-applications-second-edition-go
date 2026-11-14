package objectstorage

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
)

type Engine struct {
	Reader Reader
	Hook   func(string) error
}

func (e Engine) phase(name string) error {
	if e.Hook != nil {
		return e.Hook(name)
	}
	return nil
}
func immutable(ctx context.Context, s Store, key string, body []byte) error {
	_, err := s.Put(ctx, key, body, Condition{Absent: true})
	if err == nil {
		return nil
	}
	prior, getErr := s.Get(ctx, key)
	if getErr == nil {
		if string(prior.Body) != string(body) {
			return ErrCorrupt
		}
		return nil
	}
	return errors.Join(ErrUncertain, err, getErr)
}
func (e Engine) resolve(ctx context.Context, c Command, hash string) (Publication, bool, error) {
	h, _, err := e.Reader.Head(ctx)
	if errors.Is(err, ErrNotFound) {
		return Publication{}, false, nil
	}
	if err != nil {
		return Publication{}, false, err
	}
	m, err := e.Reader.manifest(ctx, h.Current)
	if err != nil {
		return Publication{}, false, err
	}
	for _, receipt := range m.Receipts {
		if receipt.ID != c.ID {
			continue
		}
		if receipt.Hash != hash {
			return Publication{}, false, ErrIntent
		}
		ref := h.Current
		for ref.Generation > receipt.Generation {
			ref = m.Parent
			m, err = e.Reader.manifest(ctx, ref)
			if err != nil {
				return Publication{}, false, err
			}
		}
		if ref.Generation != receipt.Generation || m.Receipts[len(m.Receipts)-1] != receipt {
			return Publication{}, false, ErrCorrupt
		}
		return Publication{ref, true}, true, nil
	}
	return Publication{}, false, nil
}

// Publish never retries a stale computed generation implicitly. A caller may
// explicitly rebase the same set-value intent after a reported CAS conflict.
// Ambiguous responses are reconciled by intent before any retry is admitted.
func (e Engine) Publish(ctx context.Context, input Command) (Publication, error) {
	c, hash, err := canonicalCommand(input)
	if err != nil {
		return Publication{}, err
	}
	if result, found, err := e.resolve(ctx, c, hash); err != nil || found {
		return result, err
	}
	h, version, err := e.Reader.Head(ctx)
	empty := errors.Is(err, ErrNotFound)
	if err != nil && !empty {
		return Publication{}, err
	}
	m := Manifest{Format: 1, Generation: 1, Segments: []SegmentRef{}, Receipts: []Receipt{}}
	base := Snapshot{Rows: []Record{}}
	if !empty {
		prior, err := e.Reader.manifest(ctx, h.Current)
		if err != nil {
			return Publication{}, err
		}
		// The initial resolve and this head read can straddle a competing commit.
		for _, receipt := range prior.Receipts {
			if receipt.ID == c.ID {
				result, _, err := e.resolve(ctx, c, hash)
				return result, err
			}
		}
		if h.Current.Generation >= MaxGenerations {
			return Publication{}, ErrLimit
		}
		base, err = e.Reader.Read(ctx, h.Current)
		if err != nil {
			return Publication{}, err
		}
		m.Generation = h.Current.Generation + 1
		m.Parent = h.Current
		m.Segments = append(m.Segments, prior.Segments...)
		m.Receipts = append(m.Receipts, prior.Receipts...)
	}
	state := map[string]Record{}
	for _, row := range base.Rows {
		state[row.Key] = row
	}
	for _, row := range c.Changes {
		if row.Tombstone {
			delete(state, row.Key)
		} else {
			state[row.Key] = row
		}
	}
	if len(state) > MaxLiveRows {
		return Publication{}, ErrLimit
	}
	rows := c.Changes
	if c.Kind == "compact" {
		rows = make([]Record, 0, len(state))
		for _, row := range state {
			rows = append(rows, row)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
		m.Segments = []SegmentRef{}
	}
	if len(m.Segments)+(len(rows)+MaxRecordsPerSegment-1)/MaxRecordsPerSegment > MaxSegments {
		return Publication{}, ErrLimit
	}
	for start := 0; start < len(rows); start += MaxRecordsPerSegment {
		end := min(start+MaxRecordsPerSegment, len(rows))
		body, _ := json.Marshal(Segment{1, rows[start:end]})
		digest := sum(body)
		key := "segments/" + digest + ".json"
		if err = immutable(ctx, e.Reader.Store, key, body); err != nil {
			return Publication{}, err
		}
		m.Segments = append(m.Segments, SegmentRef{key, digest, len(body), end - start})
	}
	if err = e.phase("segments"); err != nil {
		return Publication{}, err
	}
	m.Receipts = append(m.Receipts, Receipt{c.ID, hash, m.Generation})
	body, _ := json.Marshal(m)
	digest := sum(body)
	ref := Ref{m.Generation, "manifests/" + digest + ".json", digest}
	if err = immutable(ctx, e.Reader.Store, ref.Key, body); err != nil {
		return Publication{}, err
	}
	if err = e.phase("manifest"); err != nil {
		return Publication{}, err
	}
	if err = e.phase("before_head"); err != nil {
		return Publication{}, err
	}
	body, _ = json.Marshal(Head{1, ref})
	condition := Condition{Absent: empty, Version: version}
	_, err = e.Reader.Store.Put(ctx, "head.json", body, condition)
	if err != nil {
		result, found, resolveErr := e.resolve(ctx, c, hash)
		if errors.Is(resolveErr, ErrIntent) {
			return Publication{}, ErrIntent
		}
		if resolveErr != nil {
			return Publication{}, errors.Join(ErrUncertain, resolveErr)
		}
		if found {
			return result, nil
		}
		if errors.Is(err, ErrConflict) {
			return Publication{}, ErrConflict
		}
		if errors.Is(err, ErrAccess) {
			return Publication{}, err
		}
		return Publication{}, errors.Join(ErrUncertain, err)
	}
	if err = e.phase("after_head"); err != nil {
		return Publication{}, errors.Join(ErrUncertain, err)
	}
	return Publication{ref, false}, nil
}

type Inventory struct {
	Total     int      `json:"objects"`
	Reachable int      `json:"reachable_objects"`
	Orphans   []string `json:"orphan_candidates"`
}

// Audit is a quiescent inventory, never a garbage collector or a substitute for
// conditional publication. All history and named snapshot references are kept.
func (e Engine) Audit(ctx context.Context) (Inventory, error) {
	keys, err := e.Reader.Store.List(ctx, "")
	if err != nil {
		return Inventory{}, err
	}
	live := map[string]bool{}
	refs := []Ref{}
	h, _, err := e.Reader.Head(ctx)
	if err == nil {
		live["head.json"] = true
		refs = append(refs, h.Current)
	} else if !errors.Is(err, ErrNotFound) {
		return Inventory{}, err
	}
	for _, key := range keys {
		if len(key) > 5 && key[:5] == "pins/" {
			o, err := e.Reader.Store.Get(ctx, key)
			if err != nil {
				return Inventory{}, err
			}
			var pin Head
			if strictJSON(o.Body, &pin) != nil || pin.Format != 1 || !validRef(pin.Current) {
				return Inventory{}, ErrCorrupt
			}
			live[key] = true
			refs = append(refs, pin.Current)
		}
	}
	for len(refs) > 0 {
		ref := refs[len(refs)-1]
		refs = refs[:len(refs)-1]
		if live[ref.Key] {
			continue
		}
		m, err := e.Reader.manifest(ctx, ref)
		if err != nil {
			return Inventory{}, err
		}
		live[ref.Key] = true
		if m.Parent.Generation > 0 {
			refs = append(refs, m.Parent)
		}
		for _, segment := range m.Segments {
			live[segment.Key] = true
		}
	}
	orphans := []string{}
	for _, key := range keys {
		if !live[key] {
			orphans = append(orphans, key)
		}
	}
	return Inventory{len(keys), len(live), orphans}, nil
}
