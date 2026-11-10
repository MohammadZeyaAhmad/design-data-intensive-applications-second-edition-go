//go:build !linux

package columnstore

func processCPU() *int64 { return nil }
