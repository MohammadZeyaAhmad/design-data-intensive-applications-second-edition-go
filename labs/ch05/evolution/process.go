package evolution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type boundedBuffer struct {
	bytes.Buffer
	Limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.Limit-b.Len() {
		return 0, errors.New("process output exceeded bound")
	}
	return b.Buffer.Write(p)
}
func childEnvironment(values map[string]string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "ATLAS_") {
			env = append(env, entry)
		}
	}
	for key, value := range values {
		env = append(env, key+"="+value)
	}
	return env
}
func runCommand(ctx context.Context, binary, dir string, env map[string]string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = dir
	cmd.Env = childEnvironment(env)
	stdout, stderr := &boundedBuffer{Limit: 1 << 20}, &boundedBuffer{Limit: 8192}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if e := cmd.Run(); e != nil {
		return nil, fmt.Errorf("%s failed: %s", args[0], stderr.String())
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

type server struct {
	cmd         *exec.Cmd
	done        chan error
	base, token string
	log         *boundedBuffer
}

func startServer(ctx context.Context, binary, dir string, env map[string]string, token string) (*server, error) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return nil, e
	}
	address := listener.Addr().String()
	listener.Close()
	env["ATLAS_HTTP_ADDR"] = address
	cmd := exec.Command(binary, "serve")
	cmd.Dir = dir
	cmd.Env = childEnvironment(env)
	log := &boundedBuffer{Limit: 16384}
	cmd.Stdout = io.Discard
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		return nil, e
	}
	s := &server{cmd: cmd, done: make(chan error, 1), base: "http://" + address, token: token, log: log}
	go func() { s.done <- cmd.Wait() }()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-s.done:
			return nil, errors.New("Atlas server exited before readiness")
		default:
		}
		request, e := http.NewRequestWithContext(ctx, "GET", s.base+"/readyz", nil)
		if e != nil {
			s.stop()
			return nil, e
		}
		response, e := (&http.Client{Timeout: time.Second}).Do(request)
		if e == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				return s, nil
			}
		}
		select {
		case <-ctx.Done():
			s.stop()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	s.stop()
	return nil, errors.New("Atlas server readiness timed out")
}
func (s *server) stop() {
	if s == nil || s.cmd == nil {
		return
	}
	s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		s.cmd.Process.Kill()
		<-s.done
	}
	s.cmd = nil
}

type brand struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

func (s *server) call(ctx context.Context, method, id, key string, body any) (brand, time.Duration, error) {
	var result brand
	var data []byte
	var e error
	if body != nil {
		data, e = json.Marshal(body)
		if e != nil {
			return result, 0, e
		}
	}
	request, e := http.NewRequestWithContext(ctx, method, s.base+"/v1/catalog/brands/"+id, bytes.NewReader(data))
	if e != nil {
		return result, 0, e
	}
	request.Header.Set("Authorization", "Bearer "+s.token)
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	start := time.Now()
	response, e := (&http.Client{Timeout: 2 * time.Second}).Do(request)
	elapsed := time.Since(start)
	if e != nil {
		return result, elapsed, errors.New("brand HTTP request failed")
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, 8193))
	if e != nil || len(raw) > 8192 {
		return result, elapsed, errors.New("brand HTTP response bound exceeded")
	}
	if response.StatusCode != 200 {
		return result, elapsed, fmt.Errorf("brand HTTP status %d: %s", response.StatusCode, raw)
	}
	if e = json.Unmarshal(raw, &result); e != nil {
		return result, elapsed, e
	}
	return result, elapsed, nil
}
func (s *server) put(ctx context.Context, id, key, name string, version int64) (brand, time.Duration, error) {
	return s.call(ctx, "PUT", id, key, map[string]any{"name": name, "expected_version": version})
}

type writeSample struct {
	Binary    string `json:"binary"`
	WallNanos int64  `json:"wall_nanos"`
	Revision  int64  `json:"revision"`
}
type writerResult struct {
	Samples []writeSample
	Err     error
}

func foregroundWriters(parent context.Context, old, current *server, initial brand) func() writerResult {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan writerResult, 1)
	go func() {
		result := writerResult{}
		state := initial
		for i := 0; i < 1024; i++ {
			select {
			case <-ctx.Done():
				done <- result
				return
			default:
			}
			peer, label := old, "historical"
			if i%2 == 1 {
				peer, label = current, "current"
			}
			// Cancellation drains a current request rather than abandoning an outcome.
			updated, elapsed, e := peer.put(parent, "maker-001", fmt.Sprintf("foreground-%03d", i), fmt.Sprintf("Foreground %03d", i), state.Version)
			if e != nil {
				result.Err = e
				done <- result
				return
			}
			state = updated
			result.Samples = append(result.Samples, writeSample{label, elapsed.Nanoseconds(), updated.Version})
			if pause(ctx, 10*time.Millisecond) != nil {
				done <- result
				return
			}
		}
		done <- result
	}()
	var once sync.Once
	var result writerResult
	return func() writerResult { once.Do(func() { cancel(); result = <-done }); return result }
}
