package domain

import (
	"errors"
	"testing"
)

const testCheckout = "11111111-1111-4111-8111-111111111111"
const testOrder = "22222222-2222-4222-8222-222222222222"
const testAllocation = "33333333-3333-4333-8333-333333333333"

func TestShipmentLifecycleAndRejectedMutations(t *testing.T) {
	s, err := Create("acme", ShipmentID(testCheckout, "east"), testOrder, testCheckout, "east", []Allocation{{ID: testAllocation, SKU: "BOOK", Quantity: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Change("dispatch", "TRACK-1"); !errors.Is(err, ErrConflict) {
		t.Fatal("dispatch before packing accepted", err)
	}
	for _, action := range []string{"pick", "pack"} {
		if err = s.Change(action, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Change("dispatch", "bad reference"); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid reference accepted")
	}
	if err = s.Change("dispatch", "TRACK-1"); err != nil {
		t.Fatal(err)
	}
	if err = s.Change("deliver", ""); !errors.Is(err, ErrConflict) {
		t.Fatal("delivery before handoff accepted")
	}
	if err = s.MarkDispatched(); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkDispatched(); !errors.Is(err, ErrConflict) {
		t.Fatal("second physical handoff accepted")
	}
	if err = s.Change("deliver", ""); err != nil {
		t.Fatal(err)
	}
	v := s.Snapshot()
	v.Lines[0].Quantity = 999
	if s.Snapshot().Lines[0].Quantity != 2 || s.Snapshot().Status != Delivered || s.Snapshot().Version != 6 {
		t.Fatal("invalid terminal state or aliased snapshot")
	}
	if ShipmentID(testCheckout, "") != ShipmentID(testCheckout, "default") {
		t.Fatal("legacy warehouse identity changed")
	}
}
