package singleleader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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

type NativeOptions struct{ PGBin, PGShare, Atlas, Helper, Workdir string }
type Observation struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}
type ModeEvidence struct {
	Mode             Mode   `json:"mode"`
	Acknowledged     int    `json:"acknowledged"`
	LostAcknowledged int    `json:"lost_acknowledged"`
	PrimaryLSN       string `json:"primary_lsn"`
	ReceivedLSN      string `json:"received_lsn"`
	ReplayedLSN      string `json:"replayed_lsn"`
	ReceiveLagBytes  int64  `json:"receive_lag_bytes"`
	ReplayLagBytes   int64  `json:"replay_lag_bytes"`
	ReplyMillis      int64  `json:"reply_millis"`
}
type NativeReport struct {
	Postgres     string         `json:"postgres"`
	Model        ModelReport    `json:"model"`
	Modes        []ModeEvidence `json:"modes"`
	Observations []Observation  `json:"observations"`
	Assumptions  []string       `json:"assumptions"`
}

func (r *NativeReport) record(name string, value any) {
	r.Observations = append(r.Observations, Observation{name, value})
}

type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	once sync.Once
}

func launch(ctx context.Context, dir, logPath, binary string, env map[string]string, args ...string) (*process, error) {
	cmd := exec.Command(binary, args...)
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = ownedEnvironment(env)
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	p := &process{cmd: cmd, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); log.Close(); close(p.done) }()
	go func() {
		select {
		case <-ctx.Done():
			p.stop(true)
		case <-p.done:
		}
	}()
	return p, nil
}

// An inherited PGPASSWORD or PGSERVICE must never redirect or authenticate a
// fixture command against the user's normal database configuration.
func ownedEnvironment(values map[string]string) []string {
	var env []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "ATLAS_") && !strings.HasPrefix(e, "PG") {
			env = append(env, e)
		}
	}
	for k, v := range values {
		env = append(env, k+"="+v)
	}
	return env
}
func (p *process) stop(crash bool) {
	if p == nil {
		return
	}
	p.once.Do(func() {
		select {
		case <-p.done:
			return
		default:
		}
		if crash {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		} else {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		select {
		case <-p.done:
		case <-time.After(3 * time.Second):
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
	})
}

type node struct {
	dir  string
	port int
	proc *process
}
type fixture struct {
	ctx                                             context.Context
	options                                         NativeOptions
	dir, archive, passfile                          string
	adminPassword, appPassword, replPassword, token string
	a, b                                            *node
	processes                                       []*process
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
func randomToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func shell(s string) string   { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func (f *fixture) url(n *node, user string, password bool) string {
	u := &url.URL{Scheme: "postgresql", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(n.port)), Path: "/postgres"}
	if password {
		p := f.adminPassword
		if user == "atlas_app" {
			p = f.appPassword
		}
		if user == "atlas_repl" {
			p = f.replPassword
		}
		u.User = url.UserPassword(user, p)
	} else {
		u.User = url.User(user)
	}
	q := url.Values{"sslmode": {"disable"}, "connect_timeout": {"2"}, "application_name": {"atlas_replica"}}
	if !password {
		q.Set("passfile", f.passfile)
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func (f *fixture) connect(n *node) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, f.url(n, "atlas_admin", true))
	if err != nil {
		return nil, errors.New("owned PostgreSQL endpoint unavailable")
	}
	_, err = c.Exec(ctx, "SET synchronous_commit=local")
	if err != nil {
		c.Close(context.Background())
		return nil, err
	}
	return c, nil
}
func (f *fixture) exec(n *node, sql string, args ...any) error {
	c, err := f.connect(n)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	_, err = c.Exec(f.ctx, sql, args...)
	return err
}
func (f *fixture) scalar(n *node, sql string, dest any, args ...any) error {
	c, err := f.connect(n)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	return c.QueryRow(f.ctx, sql, args...).Scan(dest)
}
func wait(ctx context.Context, what string, condition func() (bool, error)) error {
	deadline, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := condition()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if ok {
			return nil
		}
		select {
		case <-deadline.Done():
			return fmt.Errorf("%s: %w", what, deadline.Err())
		case <-ticker.C:
		}
	}
}
func (f *fixture) command(binary string, args ...string) error {
	ctx, cancel := context.WithTimeout(f.ctx, 50*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(f.options.PGBin, binary), args...)
	cmd.Dir = f.dir
	cmd.Env = ownedEnvironment(map[string]string{"PGPASSFILE": f.passfile})
	output, err := cmd.CombinedOutput()
	if err != nil {
		s := string(output)
		for _, secret := range []string{f.adminPassword, f.replPassword, f.appPassword} {
			s = strings.ReplaceAll(s, secret, "[redacted]")
		}
		if len(s) > 2500 {
			s = s[len(s)-2500:]
		}
		return fmt.Errorf("%s failed: %s", binary, s)
	}
	return nil
}
func (f *fixture) close() {
	for i := len(f.processes) - 1; i >= 0; i-- {
		f.processes[i].stop(false)
	}
	os.RemoveAll(f.dir)
}
func newFixture(ctx context.Context, o NativeOptions) (*fixture, error) {
	dir, err := os.MkdirTemp(o.Workdir, "atlas-repl-")
	if err != nil {
		return nil, err
	}
	f := &fixture{ctx: ctx, options: o, dir: dir, archive: filepath.Join(dir, "archive"), passfile: filepath.Join(dir, "pgpass")}
	good := false
	defer func() {
		if !good {
			f.close()
		}
	}()
	for _, target := range []*string{&f.adminPassword, &f.appPassword, &f.replPassword, &f.token} {
		*target, err = randomToken()
		if err != nil {
			return nil, err
		}
	}
	if err = os.Mkdir(f.archive, 0700); err != nil {
		return nil, err
	}
	pass := "127.0.0.1:*:*:atlas_admin:" + f.adminPassword + "\n127.0.0.1:*:*:atlas_repl:" + f.replPassword + "\n"
	if err = os.WriteFile(f.passfile, []byte(pass), 0600); err != nil {
		return nil, err
	}
	aPort, err := freePort()
	if err != nil {
		return nil, err
	}
	bPort, err := freePort()
	if err != nil {
		return nil, err
	}
	f.a = &node{dir: filepath.Join(dir, "primary"), port: aPort}
	f.b = &node{dir: filepath.Join(dir, "standby"), port: bPort}
	pwfile := filepath.Join(dir, "init-password")
	if err = os.WriteFile(pwfile, []byte(f.adminPassword), 0600); err != nil {
		return nil, err
	}
	args := []string{"-D", f.a.dir, "--auth-local=trust", "--auth-host=scram-sha-256", "--pwfile=" + pwfile, "-U", "atlas_admin", "--no-locale", "--encoding=UTF8", "--data-checksums"}
	if o.PGShare != "" {
		args = append(args, "-L", o.PGShare)
	}
	if err = f.command("initdb", args...); err != nil {
		return nil, err
	}
	archiveCommand := shell(o.Helper) + " wal-archive " + shell(f.archive) + " '%p' '%f'"
	conf := "\nlisten_addresses='127.0.0.1'\nshared_buffers='16MB'\nmax_connections=32\nwal_level=replica\nmax_wal_senders=8\nmax_replication_slots=4\nwal_keep_size='16MB'\nmin_wal_size='32MB'\nmax_wal_size='128MB'\nwal_log_hints=on\narchive_mode=on\narchive_command=" + literal(archiveCommand) + "\nfull_page_writes=on\nfsync=on\nsynchronous_commit=on\nlogging_collector=off\n"
	if err = appendConfig(f.a.dir, conf); err != nil {
		return nil, err
	}
	if err = f.start(f.a); err != nil {
		return nil, err
	}
	if err = f.exec(f.a, "CREATE ROLE atlas_repl LOGIN REPLICATION PASSWORD "+literal(f.replPassword)+"; CREATE ROLE atlas_app LOGIN PASSWORD "+literal(f.appPassword)); err != nil {
		return nil, err
	}
	if err = f.migrate(); err != nil {
		return nil, err
	}
	if err = f.exec(f.a, `GRANT USAGE ON SCHEMA inventory, platform TO atlas_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA inventory TO atlas_app;
GRANT SELECT ON public.atlas_schema_migrations TO atlas_app;
UPDATE platform.writer_fence SET enabled=true;`); err != nil {
		return nil, err
	}
	good = true
	return f, nil
}
func appendConfig(dir, config string) error {
	f, err := os.OpenFile(filepath.Join(dir, "postgresql.auto.conf"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(config)
	return err
}
func (f *fixture) start(n *node) error {
	p, err := launch(f.ctx, f.dir, filepath.Join(f.dir, fmt.Sprintf("postgres-%d.log", n.port)), filepath.Join(f.options.PGBin, "postgres"), nil, "-D", n.dir, "-p", strconv.Itoa(n.port), "-k", f.dir)
	if err != nil {
		return err
	}
	n.proc = p
	f.processes = append(f.processes, p)
	return wait(f.ctx, "postmaster readiness", func() (bool, error) {
		select {
		case <-p.done:
			data, _ := os.ReadFile(filepath.Join(f.dir, fmt.Sprintf("postgres-%d.log", n.port)))
			return false, fmt.Errorf("postmaster exited: %.2500s", data)
		default:
		}
		c, err := f.connect(n)
		if err != nil {
			return false, nil
		}
		c.Close(context.Background())
		return true, nil
	})
}
func (f *fixture) migrate() error {
	p, err := launch(f.ctx, f.dir, filepath.Join(f.dir, "migrate.log"), f.options.Atlas, map[string]string{"ATLAS_DATABASE_URL": f.url(f.a, "atlas_admin", true)}, "migrate")
	if err != nil {
		return err
	}
	f.processes = append(f.processes, p)
	select {
	case <-f.ctx.Done():
		return f.ctx.Err()
	case <-p.done:
		if p.err != nil {
			b, _ := os.ReadFile(filepath.Join(f.dir, "migrate.log"))
			return fmt.Errorf("Atlas migration failed: %.2000s", b)
		}
		return nil
	}
}

type api struct {
	proc        *process
	base, token string
}

func (f *fixture) api(n *node, epoch int64) (*api, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	values := map[string]string{"ATLAS_DATABASE_URL": f.url(n, "atlas_app", true), "ATLAS_DATABASE_WRITER_EPOCH": strconv.FormatInt(epoch, 10), "ATLAS_TENANT_KEYS": "replication=" + f.token, "ATLAS_PROVIDER_TOKEN": f.token, "ATLAS_GRAPHQL_CURSOR_KEY": f.token, "ATLAS_HTTP_ADDR": address}
	p, err := launch(f.ctx, f.dir, filepath.Join(f.dir, fmt.Sprintf("api-%d.log", port)), f.options.Atlas, values, "serve")
	if err != nil {
		return nil, err
	}
	f.processes = append(f.processes, p)
	a := &api{p, "http://" + address, f.token}
	err = wait(f.ctx, "Atlas HTTP readiness", func() (bool, error) {
		select {
		case <-p.done:
			b, _ := os.ReadFile(filepath.Join(f.dir, fmt.Sprintf("api-%d.log", port)))
			return false, fmt.Errorf("Atlas exited: %.1500s", b)
		default:
		}
		code, _, e := a.request(f.ctx, "GET", "/healthz", "", "")
		return e == nil && code == 200, nil
	})
	return a, err
}
func (a *api) request(ctx context.Context, method, path, key, body string) (int, []byte, error) {
	r, err := http.NewRequestWithContext(ctx, method, a.base+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	r.Header.Set("Authorization", "Bearer "+a.token)
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	client := &http.Client{Timeout: 12 * time.Second}
	response, err := client.Do(r)
	if err != nil {
		return 0, nil, errors.New("owned Atlas HTTP request failed")
	}
	defer response.Body.Close()
	b, err := io.ReadAll(io.LimitReader(response.Body, 32768))
	return response.StatusCode, b, err
}
func (a *api) receipt(ctx context.Context, key string) error {
	code, body, err := a.request(ctx, "POST", "/v1/stock/BOOK/receipts", key, `{"quantity":1}`)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("Inventory receipt %s: HTTP %d %.600s", key, code, body)
	}
	return nil
}
func (f *fixture) bootstrap() error {
	if err := f.command("pg_basebackup", "-d", f.url(f.a, "atlas_repl", false), "-D", f.b.dir, "-R", "-X", "stream", "--checkpoint=fast", "-C", "-S", "atlas_standby"); err != nil {
		return err
	}
	if err := appendConfig(f.b.dir, "\narchive_mode=off\n"); err != nil {
		return err
	}
	if err := f.start(f.b); err != nil {
		return err
	}
	return f.catchup(f.a, f.b)
}
func (f *fixture) catchup(primary, standby *node) error {
	var lsn string
	if err := f.scalar(primary, "SELECT pg_current_wal_flush_lsn()::text", &lsn); err != nil {
		return err
	}
	return wait(f.ctx, "standby replay", func() (bool, error) {
		var caught bool
		err := f.scalar(standby, "SELECT coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn,false)", &caught, lsn)
		return caught, err
	})
}
func (f *fixture) receiptCount(n *node, key string) (int, error) {
	var count int
	err := f.scalar(n, "SELECT count(*) FROM inventory.command_results WHERE tenant_id='replication' AND idempotency_key=$1", &count, key)
	return count, err
}
func (f *fixture) fence(primary *node) error {
	primary.proc.stop(true)
	// A successful TCP connection or surviving postmaster means fencing failed.
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(primary.port)), 100*time.Millisecond)
	if err == nil {
		conn.Close()
		return errors.New("old primary still reachable; promotion refused")
	}
	return nil
}
func (f *fixture) promote(n *node, epoch int64) error {
	if err := f.command("pg_ctl", "-D", n.dir, "-w", "promote"); err != nil {
		return err
	}
	if err := f.exec(n, "UPDATE platform.writer_fence SET epoch=$1", epoch); err != nil {
		return err
	}
	return nil
}
func (f *fixture) lags() (ModeEvidence, error) {
	r := ModeEvidence{}
	if err := f.scalar(f.a, "SELECT pg_current_wal_flush_lsn()::text", &r.PrimaryLSN); err != nil {
		return r, err
	}
	c, err := f.connect(f.b)
	if err != nil {
		return r, err
	}
	defer c.Close(context.Background())
	err = c.QueryRow(f.ctx, `SELECT coalesce(pg_last_wal_receive_lsn(),'0/0')::text,coalesce(pg_last_wal_replay_lsn(),'0/0')::text,
GREATEST(0,pg_wal_lsn_diff($1::pg_lsn,coalesce(pg_last_wal_receive_lsn(),'0/0')))::bigint,
GREATEST(0,pg_wal_lsn_diff($1::pg_lsn,coalesce(pg_last_wal_replay_lsn(),'0/0')))::bigint`, r.PrimaryLSN).Scan(&r.ReceivedLSN, &r.ReplayedLSN, &r.ReceiveLagBytes, &r.ReplayLagBytes)
	return r, err
}
func validateOptions(o NativeOptions) error {
	if os.Geteuid() == 0 {
		return errors.New("replication lab must run as an unprivileged user")
	}
	for _, p := range []string{o.PGBin, o.Atlas, o.Helper} {
		if !filepath.IsAbs(p) {
			return errors.New("PostgreSQL directory and executable paths must be absolute")
		}
	}
	for _, name := range []string{"postgres", "initdb", "pg_ctl", "pg_basebackup", "pg_verifybackup", "pg_rewind"} {
		st, err := os.Stat(filepath.Join(o.PGBin, name))
		if err != nil || st.Mode()&0111 == 0 {
			return fmt.Errorf("required PostgreSQL binary unavailable: %s", name)
		}
	}
	return nil
}
func RunNative(ctx context.Context, o NativeOptions) (NativeReport, error) {
	r := NativeReport{Assumptions: []string{"isolated loopback PostgreSQL processes share one host, disk and failure domain", "fsync is enabled; tmpfs process crashes do not establish host power-loss persistence", "manual promotion requires confirmed process fencing; no automatic election or quorum service", "Inventory epoch triggers reject stale trusted application processes on the promoted database", "epochs do not fence a disconnected database; other bounded contexts are not yet epoch guarded", "RTO measures this local fixture, not a production recovery objective"}}
	if err := validateOptions(o); err != nil {
		return r, err
	}
	model, err := Demonstrate()
	if err != nil {
		return r, err
	}
	r.Model = model
	cmd := exec.CommandContext(ctx, filepath.Join(o.PGBin, "postgres"), "--version")
	version, err := cmd.Output()
	if err != nil {
		return r, errors.New("PostgreSQL runtime unavailable")
	}
	r.Postgres = strings.TrimSpace(string(version))
	if !strings.Contains(r.Postgres, " 18.") {
		return r, errors.New("PostgreSQL 18 is required")
	}
	for _, mode := range []Mode{Async, RemoteFlush, RemoteApply} {
		fmt.Fprintln(os.Stderr, "replication:", mode)
		if err = runMode(ctx, o, mode, &r); err != nil {
			return r, fmt.Errorf("%s: %w", mode, err)
		}
	}
	fmt.Fprintln(os.Stderr, "replication: backup and point-in-time recovery")
	if err = runBackup(ctx, o, &r); err != nil {
		return r, fmt.Errorf("backup/PITR: %w", err)
	}
	return r, nil
}
