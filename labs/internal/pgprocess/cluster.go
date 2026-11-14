// Package pgprocess owns disposable PostgreSQL processes for native labs.
package pgprocess

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

type Options struct{ Bin, Share, Workdir string }
type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	once sync.Once
}
type Node struct {
	Dir     string
	Port    int
	process *process
}
type Cluster struct {
	Context                 context.Context
	Options                 Options
	Dir, Password, Passfile string
	Nodes                   []*Node
	processes               []*process
}

func Literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func New(ctx context.Context, o Options) (*Cluster, error) {
	if os.Geteuid() == 0 || !filepath.IsAbs(o.Bin) {
		return nil, errors.New("PostgreSQL labs require an absolute runtime and an unprivileged user")
	}
	for _, name := range []string{"postgres", "initdb", "pg_ctl", "pg_basebackup", "pg_controldata"} {
		info, err := os.Stat(filepath.Join(o.Bin, name))
		if err != nil || info.Mode()&0111 == 0 {
			return nil, fmt.Errorf("required PostgreSQL executable unavailable: %s", name)
		}
	}
	dir, err := os.MkdirTemp(o.Workdir, "atlas-pg-")
	if err != nil {
		return nil, err
	}
	token := make([]byte, 24)
	if _, err = rand.Read(token); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	c := &Cluster{Context: ctx, Options: o, Dir: dir, Password: hex.EncodeToString(token), Passfile: filepath.Join(dir, "pgpass")}
	if err = os.WriteFile(c.Passfile, []byte("127.0.0.1:*:*:atlas_lab:"+c.Password+"\n"), 0600); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
func (c *Cluster) Close() {
	for i := len(c.processes) - 1; i >= 0; i-- {
		c.processes[i].stop()
	}
	os.RemoveAll(c.Dir)
}
func (c *Cluster) Stop(n *Node) {
	if n.process != nil {
		n.process.stop()
	}
}
func (p *process) stop() {
	p.once.Do(func() {
		select {
		case <-p.done:
			return
		default:
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
	})
}
func environment(passfile string) []string {
	out := []string{}
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "ATLAS_") && !strings.HasPrefix(e, "PG") {
			out = append(out, e)
		}
	}
	return append(out, "PGPASSFILE="+passfile)
}
func (c *Cluster) Command(name string, args ...string) error {
	ctx, cancel := context.WithTimeout(c.Context, 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, filepath.Join(c.Options.Bin, name), args...)
	command.Env = environment(c.Passfile)
	command.Dir = c.Dir
	b, err := command.CombinedOutput()
	if err != nil {
		text := strings.ReplaceAll(string(b), c.Password, "[redacted]")
		if len(text) > 2000 {
			text = text[len(text)-2000:]
		}
		return fmt.Errorf("%s: %s", name, text)
	}
	return nil
}
func (c *Cluster) Reserve(name string) (*Node, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	for _, n := range c.Nodes {
		if n.Port == port {
			return c.Reserve(name)
		}
	}
	n := &Node{Dir: filepath.Join(c.Dir, name), Port: port}
	c.Nodes = append(c.Nodes, n)
	return n, nil
}
func (c *Cluster) Init(name string) (*Node, error) {
	n, err := c.Reserve(name)
	if err != nil {
		return nil, err
	}
	pw := filepath.Join(c.Dir, "init-password")
	if err = os.WriteFile(pw, []byte(c.Password), 0600); err != nil {
		return nil, err
	}
	args := []string{"-D", n.Dir, "-U", "atlas_lab", "--auth-local=trust", "--auth-host=scram-sha-256", "--pwfile=" + pw, "--no-locale", "--encoding=UTF8", "--data-checksums"}
	if c.Options.Share != "" {
		args = append(args, "-L", c.Options.Share)
	}
	if err = c.Command("initdb", args...); err != nil {
		return nil, err
	}
	conf := "\nlisten_addresses='127.0.0.1'\nshared_buffers='16MB'\nmax_connections=32\nwal_level=logical\nmax_wal_senders=8\nmax_replication_slots=6\nmin_wal_size='32MB'\nmax_wal_size='64MB'\nmax_slot_wal_keep_size='128MB'\nwal_log_hints=on\nfsync=on\nfull_page_writes=on\ntimezone='UTC'\n"
	if err = c.Config(n, conf); err != nil {
		return nil, err
	}
	if err = c.Start(n); err != nil {
		return nil, err
	}
	return n, nil
}
func (c *Cluster) Config(n *Node, config string) error {
	f, err := os.OpenFile(filepath.Join(n.Dir, "postgresql.auto.conf"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(config)
	return err
}
func (c *Cluster) URL(n *Node, password bool) string {
	u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(n.Port)), Path: "/postgres", User: url.User("atlas_lab")}
	if password {
		u.User = url.UserPassword("atlas_lab", c.Password)
	}
	q := url.Values{"sslmode": {"disable"}, "connect_timeout": {"2"}}
	if !password {
		q.Set("passfile", c.Passfile)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func (c *Cluster) Connect(n *Node) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(c.Context, 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, c.URL(n, true))
	if err != nil {
		return nil, errors.New("owned PostgreSQL endpoint unavailable")
	}
	return conn, nil
}
func (c *Cluster) Exec(n *Node, sql string, args ...any) error {
	conn, err := c.Connect(n)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(c.Context, sql, args...)
	return err
}
func (c *Cluster) Scalar(n *Node, sql string, out any, args ...any) error {
	conn, err := c.Connect(n)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	return conn.QueryRow(c.Context, sql, args...).Scan(out)
}
func Wait(ctx context.Context, what string, fn func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := fn()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-ticker.C:
		}
	}
}
func (c *Cluster) Start(n *Node) error {
	command := exec.Command(filepath.Join(c.Options.Bin, "postgres"), "-D", n.Dir, "-p", strconv.Itoa(n.Port), "-k", c.Dir)
	command.Dir = c.Dir
	command.Env = environment(c.Passfile)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	logPath := filepath.Join(c.Dir, fmt.Sprintf("server-%d.log", n.Port))
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	command.Stdout = log
	command.Stderr = log
	if err = command.Start(); err != nil {
		log.Close()
		return err
	}
	p := &process{cmd: command, done: make(chan struct{})}
	n.process = p
	c.processes = append(c.processes, p)
	go func() { _ = command.Wait(); log.Close(); close(p.done) }()
	go func() {
		select {
		case <-c.Context.Done():
			p.stop()
		case <-p.done:
		}
	}()
	return Wait(c.Context, "postmaster readiness", func() (bool, error) {
		select {
		case <-p.done:
			b, _ := os.ReadFile(logPath)
			return false, fmt.Errorf("owned postmaster exited: %.2000s", b)
		default:
		}
		conn, err := c.Connect(n)
		if err != nil {
			return false, nil
		}
		conn.Close(context.Background())
		return true, nil
	})
}
func (c *Cluster) Standby(source *Node, name string) (*Node, error) {
	n, err := c.Reserve(name)
	if err != nil {
		return nil, err
	}
	if err = c.Command("pg_basebackup", "-d", c.URL(source, false), "-D", n.Dir, "-R", "-X", "stream", "--checkpoint=fast", "-C", "-S", "physical_"+name); err != nil {
		return nil, err
	}
	if err = c.Start(n); err != nil {
		return nil, err
	}
	return n, c.Catchup(source, n)
}
func (c *Cluster) Catchup(source, standby *Node) error {
	var lsn string
	if err := c.Scalar(source, "SELECT pg_current_wal_flush_lsn()::text", &lsn); err != nil {
		return err
	}
	return Wait(c.Context, "physical WAL replay", func() (bool, error) {
		var ready bool
		err := c.Scalar(standby, "SELECT coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn,false)", &ready, lsn)
		return ready, err
	})
}
