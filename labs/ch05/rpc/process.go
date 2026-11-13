package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	hp "google.golang.org/grpc/health/grpc_health_v1"
)

type boundedLog struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *boundedLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) > 32768-b.Len() {
		return 0, errors.New("child log bound exceeded")
	}
	return b.Buffer.Write(p)
}

type child struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func privateEnv(values map[string]string) []string {
	env := []string{}
	for _, s := range os.Environ() {
		if !strings.HasPrefix(s, "ATLAS_") {
			env = append(env, s)
		}
	}
	for k, v := range values {
		env = append(env, k+"="+v)
	}
	return env
}
func startChild(ctx context.Context, binary, dir string, env map[string]string, args ...string) (*child, error) {
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	cmd.Env = privateEnv(env)
	cmd.Stdout = io.Discard
	cmd.Stderr = &boundedLog{}
	if e := cmd.Start(); e != nil {
		return nil, e
	}
	c := &child{cmd: cmd, done: make(chan struct{})}
	go func() { c.err = cmd.Wait(); close(c.done) }()
	go func() {
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
		case <-c.done:
		}
	}()
	return c, nil
}
func (c *child) stop() {
	select {
	case <-c.done:
		return
	default:
	}
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
	}
}
func (c *child) kill() { _ = c.cmd.Process.Kill(); <-c.done }
func address() (string, error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", e
	}
	defer l.Close()
	return l.Addr().String(), nil
}
func dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10), grpc.MaxCallSendMsgSize(64<<10)))
}
func eventually(ctx context.Context, limit time.Duration, fn func() bool) error {
	until, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	t := time.NewTicker(15 * time.Millisecond)
	defer t.Stop()
	for {
		if fn() {
			return nil
		}
		select {
		case <-until.Done():
			return errors.New("bounded RPC condition did not converge")
		case <-t.C:
		}
	}
}
func waitHealth(ctx context.Context, c *child, target string) error {
	conn, e := dial(target)
	if e != nil {
		return e
	}
	defer conn.Close()
	h := hp.NewHealthClient(conn)
	return eventually(ctx, 15*time.Second, func() bool {
		select {
		case <-c.done:
			return false
		default:
		}
		attempt, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		r, e := h.Check(attempt, &hp.HealthCheckRequest{Service: "atlas.inventory.v1.Inventory"})
		return e == nil && r.Status == hp.HealthCheckResponse_SERVING
	})
}
func rest(ctx context.Context, base, token, method, path, key string, body any) ([]byte, http.Header, error) {
	var raw []byte
	var e error
	if body != nil {
		raw, e = json.Marshal(body)
		if e != nil {
			return nil, nil, e
		}
	}
	attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(attempt, method, base+path, bytes.NewReader(raw))
	if e != nil {
		return nil, nil, e
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, e := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if e != nil {
		return nil, nil, e
	}
	defer response.Body.Close()
	b, e := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if e != nil {
		return nil, nil, e
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, nil, errors.New("unexpected REST status")
	}
	return b, response.Header, nil
}
