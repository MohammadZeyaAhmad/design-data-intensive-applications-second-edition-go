//go:build !linux

package main

import "errors"

func limits() error { return errors.New("the bounded gob worker requires Linux") }
