//go:build linux

package columnstore

import "syscall"

// Process CPU includes other Go runtime work during the interval. It does not
// turn wall time or compressed byte requests into a device/CPU-cycle measure.
func processCPU() *int64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return nil
	}
	n := (r.Utime.Sec+r.Stime.Sec)*1e9 + (r.Utime.Usec+r.Stime.Usec)*1000
	return &n
}
