package domain

import (
	"testing"
)

func TestQuoteMoneyRejectsOverflow(t *testing.T) {
	if _, err := Add(0, 1_000_000_000_000, 1_000_000_000); err == nil {
		t.Fatal("overflowing quote accepted")
	}
	if _, err := Add(MaxTotal, 1, 1); err == nil {
		t.Fatal("aggregate overflow accepted")
	}
	if got, err := Add(100, 1250, 2); err != nil || got != 2600 {
		t.Fatal("incorrect minor-unit arithmetic")
	}
}
