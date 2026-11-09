// Package lostupdate models a concrete read/modify/write schedule without a
// Go memory data race. The database anomaly is a lost logical update.
package lostupdate

type Report struct {
	Concept                string   `json:"concept"`
	Schedule               []string `json:"schedule"`
	UnsafeFinal            int      `json:"unsafe_final"`
	AtomicFinal            int      `json:"atomic_final"`
	ExpectedCounterexample bool     `json:"expected_counterexample"`
}

func Run() Report {
	value := 0
	a := value
	b := value
	value = a + 1
	value = b + 1
	safe := 0
	for range 2 {
		safe++
	} // indivisible update operations in the modeled schedule
	return Report{Concept: "C08-03: lost updates", Schedule: []string{"A reads 0", "B reads 0", "A writes 1", "B writes 1"}, UnsafeFinal: value, AtomicFinal: safe, ExpectedCounterexample: value != safe}
}
