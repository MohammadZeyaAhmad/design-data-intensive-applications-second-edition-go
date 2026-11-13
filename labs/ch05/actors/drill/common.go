package drill

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors/checkpoint"
)

type Observation struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Observed any    `json:"observed"`
	Passed   bool   `json:"passed"`
}
type Report struct {
	Scenario     string          `json:"scenario"`
	Build        json.RawMessage `json:"binary_build"`
	Observations []Observation   `json:"observations"`
	Verified     bool            `json:"verified"`
	Boundaries   []string        `json:"boundaries"`
}

func (r *Report) check(name, expected string, value any, ok bool) error {
	r.Observations = append(r.Observations, Observation{Name: name, Expected: expected, Observed: value, Passed: ok})
	if !ok {
		return fmt.Errorf("actor observation failed: %s", name)
	}
	return nil
}
func wait(ctx context.Context, budget time.Duration, fn func() bool) error {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		if fn() {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("bounded actor checkpoint did not converge")
		case <-t.C:
		}
	}
}
func waitCheckpoint(ctx context.Context, path, stage, id string) error {
	// A killed Kafka member retains a six-second group session. Replacement
	// assignment must have time to outlive that real lease before this barrier.
	return wait(ctx, 20*time.Second, func() bool {
		event, e := checkpoint.Read(path)
		return e == nil && event.Stage == stage && event.ID == id
	})
}
func freeAddress() (string, error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", e
	}
	defer l.Close()
	return l.Addr().String(), nil
}
func nonce() (string, error) {
	var b [12]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	return hex.EncodeToString(b[:]), nil
}
func prepare(binaries, parent string) (binary, dir string, build json.RawMessage, err error) {
	binaries, err = filepath.Abs(binaries)
	if err != nil {
		return
	}
	raw, e := os.ReadFile(filepath.Join(binaries, "manifest.json"))
	if e != nil {
		err = e
		return
	}
	if len(raw) > 8192 {
		err = errors.New("actor manifest exceeds bound")
		return
	}
	var manifest struct {
		SHA string `json:"sha256"`
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		return
	}
	binary = filepath.Join(binaries, "actor-node")
	f, e := os.Open(binary)
	if e != nil {
		err = e
		return
	}
	hash := sha256.New()
	_, err = io.Copy(hash, f)
	f.Close()
	if err != nil {
		return
	}
	if hex.EncodeToString(hash.Sum(nil)) != manifest.SHA {
		err = errors.New("actor executable digest mismatch")
		return
	}
	if parent == "" {
		parent = os.TempDir()
	}
	if err = os.MkdirAll(parent, 0700); err != nil {
		return
	}
	dir, err = os.MkdirTemp(parent, "actors-run-")
	if err != nil {
		return
	}
	dir, err = filepath.Abs(dir)
	build = raw
	return
}
