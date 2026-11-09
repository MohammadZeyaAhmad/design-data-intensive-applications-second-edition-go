package domain

const (
	Reserving    = "reserving"
	Capturing    = "capturing"
	Allocating   = "allocating"
	Confirming   = "confirming"
	Compensating = "compensating"
	Confirmed    = "confirmed"
	Failed       = "failed"
	Cancelled    = "cancelled"
)

type State struct {
	Stage  string `json:"stage"`
	Index  int    `json:"index"`
	Reason string `json:"reason,omitempty"`
}

func Terminal(s string) bool { return s == Confirmed || s == Failed || s == Cancelled }
