package domain

import (
	"errors"
	"testing"
	"time"
)

func TestDeadlineBoundaryAndExplicitCompensation(t *testing.T) {
	deadline := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r, err := NewReservation("id", "acme", "BOOK", 2, deadline)
	if err != nil {
		t.Fatal(err)
	}
	before := r.Snapshot()
	if _, err = r.Transition(ReservationCommitted, deadline); !errors.Is(err, ErrReservationExpired) {
		t.Fatal("commit at deadline was accepted")
	}
	if r.Snapshot() != before {
		t.Fatal("rejected commit mutated reservation")
	}
	if _, err = r.Transition(ReservationExpired, deadline.Add(-time.Nanosecond)); !errors.Is(err, ErrReservationNotDue) {
		t.Fatal("hold expired before its deadline")
	}
	if changed, err := r.Transition(ReservationExpired, deadline); err != nil || !changed {
		t.Fatal("due hold did not expire")
	}
	if _, err = r.Transition(ReservationCommitted, deadline); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("expired hold resurrected")
	}
	r, err = NewReservation("id", "acme", "BOOK", 2, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Transition(ReservationCommitted, deadline.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Transition(ReservationReleased, deadline.Add(time.Second)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("ordinary release undid an allocation")
	}
	if changed, err := r.Transition(ReservationReversed, deadline.Add(time.Second)); err != nil || !changed {
		t.Fatal("explicit reversal failed")
	}
	if changed, err := r.Transition(ReservationReversed, deadline.Add(time.Second)); err != nil || changed {
		t.Fatal("reversal was applied twice")
	}
}
