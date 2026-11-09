package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestStockNeverOversells(t *testing.T) {
	s, err := RestoreStock(StockState{TenantID: "acme", SKU: "BOOK"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Receive(10); err != nil {
		t.Fatal(err)
	}
	if err = s.Reserve(7); err != nil {
		t.Fatal(err)
	}
	before := s.Snapshot()
	if err = s.Reserve(4); !errors.Is(err, ErrInsufficientStock) {
		t.Fatalf("expected insufficient stock, got %v", err)
	}
	if s.Snapshot() != before {
		t.Fatal("rejected command changed state")
	}
	if err = s.Commit(7); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot(); got.Reserved != 0 || got.Allocated != 7 || got.Available() != 3 {
		t.Fatalf("unexpected allocation: %+v", got)
	}
}

func TestOverflowAndCorruptStateAreRejected(t *testing.T) {
	s, err := RestoreStock(StockState{TenantID: "acme", SKU: "BOOK", OnHand: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Receive(1); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("overflow accepted: %v", err)
	}
	if _, err = RestoreStock(StockState{TenantID: "acme", SKU: "BOOK", OnHand: 10, Reserved: 8, Allocated: 3}); !errors.Is(err, ErrCorruptState) {
		t.Fatal("corrupt snapshot accepted")
	}
}

func TestReservationTerminalStates(t *testing.T) {
	r, err := NewReservation("id", "acme", "BOOK", 3, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	changed, err := r.Transition(ReservationCommitted, time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || !changed {
		t.Fatal("commit failed")
	}
	changed, err = r.Transition(ReservationCommitted, time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || changed {
		t.Fatal("duplicate commit should have no new effect")
	}
	if _, err = r.Transition(ReservationReleased, time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("ordinary release undid committed allocation")
	}
}

func FuzzStockOperations(f *testing.F) {
	f.Add([]byte{0, 1, 1, 3, 2, 255, 4})
	f.Fuzz(func(t *testing.T, ops []byte) {
		s, err := RestoreStock(StockState{TenantID: "acme", SKU: "BOOK"})
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range ops {
			q := int64(op%17) + 1
			before := s.Snapshot()
			switch op % 4 {
			case 0:
				err = s.Receive(q)
			case 1:
				err = s.Reserve(q)
			case 2:
				err = s.Release(q)
			case 3:
				err = s.Commit(q)
			}
			after := s.Snapshot()
			if err != nil && after != before {
				t.Fatal("failed command mutated inventory")
			}
			if after.OnHand < 0 || after.Reserved < 0 || after.Allocated < 0 || after.Available() < 0 {
				t.Fatalf("invalid state: %+v", after)
			}
		}
	})
}
