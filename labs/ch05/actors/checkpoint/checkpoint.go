// Package checkpoint contains lab-only fault controls. No application service
// imports it or exposes these controls.
package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"
)

type Event struct {
	Stage      string  `json:"stage"`
	ID         string  `json:"id,omitempty"`
	Worker     string  `json:"worker,omitempty"`
	Partitions []int32 `json:"partitions,omitempty"`
}

func Write(path string, event Event) error {
	if path == "" {
		return nil
	}
	b, e := json.Marshal(event)
	if e != nil {
		return e
	}
	if e = os.WriteFile(path+".tmp", b, 0600); e != nil {
		return e
	}
	return os.Rename(path+".tmp", path)
}
func Read(path string) (Event, error) {
	var out Event
	f, e := os.Open(path)
	if e != nil {
		return out, e
	}
	defer f.Close()
	if e = json.NewDecoder(f).Decode(&out); e != nil {
		return out, e
	}
	return out, nil
}
func Pause(ctx context.Context, path, release string, event Event) error {
	if path == "" || release == "" {
		return errors.New("fault checkpoint and release paths are required")
	}
	if e := Write(path, event); e != nil {
		return e
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, e := os.Stat(release); e == nil {
			return nil
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
