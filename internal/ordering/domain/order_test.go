package domain

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

const testID = "72f44666-e594-4cd8-aa66-f44e6c2dd387"

func TestReplayUpcastAndSnapshotIsolation(t *testing.T) {
	o, _ := New("acme", testID)
	legacy := Event{Version: 1, Type: OrderPlaced, SchemaVersion: 1, Data: json.RawMessage(`{"items":[{"sku":"BOOK","qty":2}]}`)}
	if err := o.Apply(legacy); err != nil {
		t.Fatal(err)
	}
	copy := o.Snapshot()
	copy.Lines[0].Quantity = 999
	if o.Snapshot().Lines[0].Quantity != 2 {
		t.Fatal("snapshot aliased aggregate state")
	}
	restored, err := Restore(o.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	events, err := o.Cancel("customer_requested")
	if err != nil {
		t.Fatal(err)
	}
	if o.Snapshot().Status != Placed {
		t.Fatal("deciding a command changed committed state")
	}
	if err = o.Apply(events[0]); err != nil {
		t.Fatal(err)
	}
	if err = restored.Apply(events[0]); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Snapshot(), o.Snapshot()) {
		t.Fatal("snapshot plus suffix differs from replay")
	}
	if _, err = o.ReplaceLines([]Line{{SKU: "BOOK", Quantity: 1}}); !errors.Is(err, ErrInvalidState) {
		t.Fatal("cancelled order accepted a mutation")
	}
}
func TestUnknownSchemaAndGapsFailWithoutMutation(t *testing.T) {
	o, _ := New("acme", testID)
	before := o.Snapshot()
	for _, e := range []Event{{Version: 2, Type: OrderPlaced, SchemaVersion: 2, Data: json.RawMessage(`{"lines":[{"sku":"BOOK","quantity":1}]}`)}, {Version: 1, Type: OrderPlaced, SchemaVersion: 99, Data: json.RawMessage(`{}`)}} {
		if err := o.Apply(e); err == nil {
			t.Fatal("unsupported history accepted")
		}
		if !reflect.DeepEqual(o.Snapshot(), before) {
			t.Fatal("failed replay mutated state")
		}
	}
}
func FuzzEventReplay(f *testing.F) {
	f.Add([]byte(`{"lines":[{"sku":"BOOK","quantity":1}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			return
		}
		o, _ := New("acme", testID)
		before := o.Snapshot()
		err := o.Apply(Event{Version: 1, Type: OrderPlaced, SchemaVersion: 2, Data: data})
		if err != nil {
			if !reflect.DeepEqual(o.Snapshot(), before) {
				t.Fatal("invalid event changed state")
			}
			return
		}
		if _, err = Restore(o.Snapshot()); err != nil {
			t.Fatal("successful event produced invalid state")
		}
	})
}
