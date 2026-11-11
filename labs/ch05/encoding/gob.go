package encoding

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
)

type limitedOutput struct {
	buffer bytes.Buffer
	max    int
}

func (b *limitedOutput) Write(p []byte) (int, error) {
	if len(p) > b.max-b.buffer.Len() {
		return 0, model.ErrBudget
	}
	return b.buffer.Write(p)
}

// Suite owns the temporary non-race gob worker. Building it is setup work, kept
// separate from query samples. Pure wire codecs remain usable without it.
type Suite struct{ GobHelper, owned string }

func NewSuite(ctx context.Context) (*Suite, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("isolated gob decoding requires Linux")
	}
	root, e := os.Getwd()
	if e != nil {
		return nil, e
	}
	module := ""
	for p := root; ; p = filepath.Dir(p) {
		for _, candidate := range []string{p, filepath.Join(p, "labs")} {
			b, e := os.ReadFile(filepath.Join(candidate, "go.mod"))
			if e == nil && strings.Contains(string(b), "second-edition-go/labs\n") {
				module = candidate
				break
			}
		}
		if module != "" || filepath.Dir(p) == p {
			break
		}
	}
	if module == "" {
		return nil, errors.New("run the encoding lab inside this repository")
	}
	dir, e := os.MkdirTemp("", "atlas-encoding-worker-")
	if e != nil {
		return nil, e
	}
	s := &Suite{GobHelper: filepath.Join(dir, "gob-decode"), owned: dir}
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", s.GobHelper, "./cmd/gob-decode")
	cmd.Dir = module
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, e := cmd.CombinedOutput(); e != nil {
		s.Close()
		return nil, errors.New("could not build bounded gob worker: " + string(output))
	}
	return s, nil
}
func (s *Suite) Close() error {
	if s.owned == "" {
		return nil
	}
	e := os.RemoveAll(s.owned)
	s.owned = ""
	return e
}
func gobEncode(version int, r model.Record) ([]byte, error) {
	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	var err error
	if version == 1 {
		err = e.Encode(model.ToGob1(r))
	} else {
		err = e.Encode(model.ToGob2(r))
	}
	return b.Bytes(), err
}
func (s *Suite) gobDecode(ctx context.Context, version int, b []byte) (model.Record, error) {
	var r model.Record
	if s == nil || s.GobHelper == "" {
		return r, errors.New("bounded gob worker is required")
	}
	limited, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(limited, s.GobHelper)
	cmd.Stdin = bytes.NewReader(append([]byte{byte(version)}, b...))
	cmd.Env = append(os.Environ(), "GOMEMLIMIT=64MiB", "GOGC=50")
	out, stderr := &limitedOutput{max: model.MaxWire * 2}, &limitedOutput{max: 4096}
	cmd.Stdout, cmd.Stderr = out, stderr
	if e := cmd.Run(); e != nil {
		if ctx.Err() != nil {
			return r, ctx.Err()
		}
		return r, model.ErrInvalid
	}
	if json.Unmarshal(out.buffer.Bytes(), &r) != nil {
		return r, model.ErrInvalid
	}
	return r, nil
}
