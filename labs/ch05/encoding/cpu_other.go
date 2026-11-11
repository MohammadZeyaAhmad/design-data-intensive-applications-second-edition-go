//go:build !linux

package encoding

import "errors"

func processCPU() (int64, error) { return 0, errors.New("process CPU accounting requires Linux") }
