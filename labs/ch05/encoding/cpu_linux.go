//go:build linux

package encoding

import "syscall"

func processCPU() (int64, error) {
	var total int64
	for _, who := range []int{syscall.RUSAGE_SELF, syscall.RUSAGE_CHILDREN} {
		var r syscall.Rusage
		if e := syscall.Getrusage(who, &r); e != nil {
			return 0, e
		}
		total += r.Utime.Nano() + r.Stime.Nano()
	}
	return total, nil
}
