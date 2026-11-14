package sessionconsistency

import "testing"

func TestReplicaSchedulesNeverRegressAcceptedSession(t *testing.T) {
	if _, e := Model(); e != nil {
		t.Fatal(e)
	}
	// Enumerate independent replay advances, writes and reads at two replicas.
	// The oracle is the maximum acknowledged/observed log index, independent
	// of the session vector implementation under test.
	for schedule := 0; schedule < 15625; schedule++ {
		n := schedule
		primary := uint64(0)
		replay := [2]uint64{}
		maxSeen := uint64(0)
		v := Vector{}
		for range 6 {
			op := n % 5
			n /= 5
			switch op {
			case 0:
				primary++
				v.Observe("inventory", primary)
				if primary > maxSeen {
					maxSeen = primary
				}
			case 1, 2:
				replay[op-1] = primary
			case 3, 4:
				p := replay[op-3]
				accepted := v.Satisfied(Vector{"inventory": p})
				if accepted != (p >= maxSeen) {
					t.Fatalf("schedule %d: replay=%d oracle=%d", schedule, p, maxSeen)
				}
				if accepted {
					v.Observe("inventory", p)
					if p > maxSeen {
						maxSeen = p
					}
				}
			}
		}
	}
}
