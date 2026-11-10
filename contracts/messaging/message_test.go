package messaging

import (
	"encoding/json"
	"testing"
)

func TestWireIdentityAndStrictBounds(t *testing.T) {
	m := Message{Version: 1, ID: "11111111-1111-4111-8111-111111111111", TenantID: "acme", StreamID: "BOOK/east", Sequence: 1, Type: "inventory.stock-received.v1", SchemaVersion: 1, Data: json.RawMessage(`{"quantity":1}`)}
	b, err := Encode(InventoryTopic, m)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(InventoryTopic, b)
	if err != nil || !Same(decoded.Data, m.Data) {
		t.Fatal("round trip", err)
	}
	if _, err = Decode(InventoryTopic, append(b, []byte(` {}`)...)); err == nil {
		t.Fatal("trailing wire document accepted")
	}
	bad := m
	bad.ID = "11111111-1111-4111-8111-11111111111Z"
	if bad.Valid(InventoryTopic) {
		t.Fatal("invalid ID accepted")
	}
	other := m
	other.TenantID = "ac"
	other.StreamID = "meBOOK/east"
	if string(other.Key()) == string(m.Key()) {
		t.Fatal("ambiguous stream key")
	}
	if Partition(m.Key()) < 0 || Partition(m.Key()) >= Partitions {
		t.Fatal("invalid partition")
	}
	if _, err = Decode(InventoryTopic, make([]byte, MaxBytes+1)); err == nil {
		t.Fatal("oversized record accepted")
	}
}
