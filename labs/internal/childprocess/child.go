// Package childprocess owns bounded, cancelable native lab processes.
package childprocess

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type logBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) > 32768-b.Len() {
		return 0, errors.New("child output bound exceeded")
	}
	return b.Buffer.Write(p)
}

type Child struct {
	command *exec.Cmd
	done    chan struct{}
	err     error
}

func Start(ctx context.Context, binary, dir string, values map[string]string, args ...string) (*Child, error) {
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "ATLAS_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	for k, v := range values {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = &logBuffer{}
	if e := cmd.Start(); e != nil {
		return nil, e
	}
	child := &Child{command: cmd, done: make(chan struct{})}
	go func() { child.err = cmd.Wait(); close(child.done) }()
	go func() {
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
		case <-child.done:
		}
	}()
	return child, nil
}
func (c *Child) Stop() {
	select {
	case <-c.done:
		return
	default:
	}
	_ = c.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		_ = c.command.Process.Kill()
		<-c.done
	}
}
func (c *Child) Kill() { _ = c.command.Process.Kill(); <-c.done }
func (c *Child) Exited() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
func (c *Child) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.err
	}
}
