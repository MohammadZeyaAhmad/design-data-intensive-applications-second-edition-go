//go:build linux

package main

import "syscall"

func limits() error {
	for kind, value := range map[int]uint64{syscall.RLIMIT_AS: 4 << 30, syscall.RLIMIT_DATA: 256 << 20, syscall.RLIMIT_CPU: 2} {
		if e := syscall.Setrlimit(kind, &syscall.Rlimit{Cur: value, Max: value}); e != nil {
			return e
		}
	}
	return nil
}
