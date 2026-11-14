package objectstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
)

type CacheEvidence struct {
	ColdGets     uint64     `json:"cold_gets"`
	WarmGets     uint64     `json:"warm_gets"`
	ReopenedGets uint64     `json:"reopened_gets"`
	ColdBytes    uint64     `json:"cold_bytes"`
	WarmBytes    uint64     `json:"warm_bytes"`
	Cache        CacheStats `json:"cache"`
}
type WorkloadEvidence struct {
	InitialRows              int           `json:"initial_rows"`
	FinalRows                int           `json:"final_rows"`
	FinalGeneration          uint64        `json:"final_generation"`
	FinalDigest              string        `json:"final_state_sha256"`
	BeforeCompactionSegments int           `json:"before_compaction_segments"`
	AfterCompactionSegments  int           `json:"after_compaction_segments"`
	Cache                    CacheEvidence `json:"cache_cost"`
	Assertions               []string      `json:"assertions"`
}

func fixtureRows() []Record {
	rows := []Record{}
	for i := range 256 {
		tenant := "acme"
		if i%2 != 0 {
			tenant = "globex"
		}
		warehouse := "east"
		if i%3 == 0 {
			warehouse = "west"
		}
		rows = append(rows, Record{Key: fmt.Sprintf("%s/%s/SKU%04d", tenant, warehouse, i), Value: int64(100 + i)})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows
}
func rowsDigest(rows []Record) string { b, _ := json.Marshal(rows); return sum(b) }
func RunWorkload(ctx context.Context, meter *Meter) (WorkloadEvidence, error) {
	r := WorkloadEvidence{InitialRows: 256}
	e := newEngine(meter)
	initial := fixtureRows()
	oracle := map[string]Record{}
	for _, row := range initial {
		oracle[row.Key] = row
	}
	first, err := e.Publish(ctx, Command{"warehouse-import", "update", initial})
	if err != nil {
		return r, err
	}
	if err = e.Reader.Pin(ctx, "before-changes", first.Ref); err != nil {
		return r, err
	}
	for i := range 8 {
		row := initial[i]
		row.Value += 1000
		if i%3 == 0 {
			row.Tombstone = true
			row.Value = 0
			delete(oracle, row.Key)
		} else {
			oracle[row.Key] = row
		}
		if _, err = e.Publish(ctx, Command{fmt.Sprintf("warehouse-change-%d", i), "update", []Record{row}}); err != nil {
			return r, err
		}
	}
	h, _, err := e.Reader.Head(ctx)
	if err != nil {
		return r, err
	}
	m, err := e.Reader.manifest(ctx, h.Current)
	if err != nil {
		return r, err
	}
	r.BeforeCompactionSegments = len(m.Segments)
	before, err := e.Reader.Current(ctx)
	if err != nil {
		return r, err
	}
	if _, err = e.Publish(ctx, Command{"warehouse-compaction", "compact", nil}); err != nil {
		return r, err
	}
	h, _, err = e.Reader.Head(ctx)
	if err != nil {
		return r, err
	}
	m, err = e.Reader.manifest(ctx, h.Current)
	if err != nil {
		return r, err
	}
	r.AfterCompactionSegments = len(m.Segments)
	current, err := e.Reader.Current(ctx)
	if err != nil {
		return r, err
	}
	expected := []Record{}
	for _, row := range oracle {
		expected = append(expected, row)
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].Key < expected[j].Key })
	if !reflect.DeepEqual(current.Rows, expected) || !reflect.DeepEqual(before.Rows, current.Rows) || r.AfterCompactionSegments >= r.BeforeCompactionSegments {
		return r, errors.New("compaction changed warehouse state or failed to reduce references")
	}
	pinned, err := e.Reader.Pinned(ctx, "before-changes")
	if err != nil || !reflect.DeepEqual(pinned.Rows, initial) {
		return r, errors.New("snapshot changed after tombstones and compaction")
	}
	retry, err := e.Publish(ctx, Command{"warehouse-import", "update", initial})
	if err != nil || !retry.Replayed || retry.Ref != first.Ref {
		return r, errors.New("historical intent reconciliation did not return original generation")
	}
	changed := append([]Record(nil), initial...)
	changed[0].Value++
	if _, err = e.Publish(ctx, Command{"warehouse-import", "update", changed}); !errors.Is(err, ErrIntent) {
		return r, errors.New("conflicting original intent admitted")
	}
	cache, _ := NewCache(2 * MaxObjectBytes)
	reader := Reader{meter, cache}
	start := meter.Snapshot()
	if _, err = reader.Current(ctx); err != nil {
		return r, err
	}
	cold := meter.Snapshot()
	if _, err = reader.Current(ctx); err != nil {
		return r, err
	}
	warm := meter.Snapshot()
	reopened := Reader{Store: meter}
	if _, err = reopened.Current(ctx); err != nil {
		return r, err
	}
	end := meter.Snapshot()
	r.Cache = CacheEvidence{cold.Gets - start.Gets, warm.Gets - cold.Gets, end.Gets - warm.Gets, cold.ReadBytes - start.ReadBytes, warm.ReadBytes - cold.ReadBytes, cache.Stats()}
	if r.Cache.WarmGets != 1 || r.Cache.ColdGets <= 1 || r.Cache.ReopenedGets != r.Cache.ColdGets {
		return r, errors.New("reader cache does not preserve fresh head reads and independent reopen")
	}
	r.FinalRows = len(current.Rows)
	r.FinalGeneration = current.Ref.Generation
	r.FinalDigest = rowsDigest(current.Rows)
	r.Assertions = []string{"independent map oracle agrees after updates and tombstones", "compaction reduces references without changing rows", "named snapshot retains original rows after compaction", "old intent returns its original immutable generation", "changed intent payload is rejected", "warm reads fetch the head while immutable bodies hit verified cache", "new reader fetches a complete independent generation"}
	return r, nil
}
func newEngine(s Store) Engine {
	cache, _ := NewCache(2 * MaxObjectBytes)
	return Engine{Reader: Reader{s, cache}}
}
