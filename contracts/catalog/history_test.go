package catalog

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestVersionedBrandHistoryAndImmutableArchive(t *testing.T) {
	old := []byte(`{"id":"maker","name":"Old maker","version":1}`)
	newer := []byte(`{"id":"maker","display_name":"Current maker","version":2}`)
	copyOld := append([]byte(nil), old...)
	var records []BrandSourceRecord
	for i, raw := range [][]byte{old, newer} {
		snapshot, e := UpcastBrandHistory(i+1, raw)
		if e != nil || snapshot.Version != int64(i+1) {
			t.Fatal(snapshot, e)
		}
		meta := []byte(`{"actor":"system:fixture"}`)
		records = append(records, BrandSourceRecord{Tenant: "acme", ID: "maker", Version: int64(i + 1), SchemaVersion: i + 1, SourceSHA256: HistoryDigest(raw), Body: raw, Metadata: meta, MetadataSHA256: HistoryDigest(meta), RecordedAt: time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)})
	}
	archive, e := EncodeBrandArchive(records)
	if e != nil {
		t.Fatal(e)
	}
	duplicate := bytes.Replace(archive, []byte(`"archive_format_version": 1`), []byte(`"archive_format_version": 1,"archive_format_version": 1`), 1)
	if bytes.Equal(duplicate, archive) {
		t.Fatal("duplicate mutation missed envelope")
	}
	if _, e = ReplayBrandArchive(duplicate); e == nil {
		t.Fatal("duplicate archive version accepted")
	}
	out, e := ReplayBrandArchive(archive)
	if e != nil || len(out) != 2 || out[0].DisplayName != "Old maker" || out[1].DisplayName != "Current maker" || !bytes.Equal(old, copyOld) {
		t.Fatal(out, e)
	}
	var decoded BrandHistoryArchive
	json.Unmarshal(archive, &decoded)
	decoded.Records[0].Body[0] = '!'
	broken, _ := json.Marshal(decoded)
	if result, e := ReplayBrandArchive(broken); e == nil || result != nil {
		t.Fatal("corrupt source returned partial replay")
	}
	for _, source := range []struct {
		version int
		raw     string
	}{{3, string(newer)}, {1, `{"id":"maker","name":"one","name":"two","version":1}`}, {2, `{"id":"maker","display_name":null,"version":2}`}, {1, `{"id":"maker","name":"n","version":1,"other":true}`}} {
		if result, e := UpcastBrandHistory(source.version, []byte(source.raw)); e == nil || !reflect.DeepEqual(result, BrandSnapshot{}) {
			t.Fatal("unsafe/future contract accepted")
		}
	}
}
func FuzzBrandHistory(f *testing.F) {
	f.Add(1, []byte(`{"id":"maker","name":"Maker","version":1}`))
	f.Add(2, []byte(`{"id":"maker","display_name":"Maker","version":2}`))
	f.Fuzz(func(t *testing.T, version int, raw []byte) {
		if len(raw) > 4096 {
			return
		}
		record, e := UpcastBrandHistory(version, raw)
		if e == nil && (record.ID == "" || record.DisplayName == "" || record.Version < 1) {
			t.Fatal("invalid success")
		}
	})
}

func FuzzBrandArchive(f *testing.F) {
	metadata := []byte(`{"actor":"fixture"}`)
	body := []byte(`{"id":"maker","name":"Maker","version":1}`)
	archive, e := EncodeBrandArchive([]BrandSourceRecord{{Tenant: "acme", ID: "maker", Version: 1, SchemaVersion: 1, Body: body, SourceSHA256: HistoryDigest(body), Metadata: metadata, MetadataSHA256: HistoryDigest(metadata), RecordedAt: time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)}})
	if e != nil {
		f.Fatal(e)
	}
	f.Add(archive)
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxBrandArchive {
			return
		}
		result, e := ReplayBrandArchive(raw)
		if e != nil && result != nil {
			t.Fatal("failed archive returned a prefix")
		}
		for _, snapshot := range result {
			if snapshot.ID == "" || snapshot.DisplayName == "" || snapshot.Version < 1 {
				t.Fatal("invalid successful snapshot")
			}
		}
	})
}
