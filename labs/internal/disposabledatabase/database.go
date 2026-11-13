package disposabledatabase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Database struct {
	Pool  *pgxpool.Pool
	DSN   string
	Name  string
	admin *pgxpool.Pool
}

func Create(ctx context.Context, dsn string) (*Database, error) {
	u, e := url.Parse(dsn)
	if e != nil || dsn == "" || u.Path != "/atlas_test" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "postgres") {
		return nil, errors.New("bind ATLAS_TEST_DATABASE_URL to local atlas_test; no implicit skip")
	}
	var nonce [12]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return nil, e
	}
	name := "atlas_actors_" + hex.EncodeToString(nonce[:])
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		return nil, e
	}
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
		admin.Close()
		return nil, errors.New("could not create owned actor database")
	}
	u.Path = "/" + name
	database := &Database{DSN: u.String(), Name: name, admin: admin}
	database.Pool, e = Open(ctx, database.DSN, "controller")
	if e != nil {
		_ = database.Close()
		return nil, e
	}
	return database, nil
}
func Open(ctx context.Context, dsn, label string) (*pgxpool.Pool, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`).MatchString(label) {
		return nil, errors.New("invalid actor database label")
	}
	u, e := url.Parse(dsn)
	if e != nil || !regexp.MustCompile(`^/atlas_actors_[0-9a-f]{24}$`).MatchString(u.Path) || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "postgres") {
		return nil, errors.New("worker database must be owned by the actor lab")
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		return nil, errors.New("invalid actor database configuration")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["application_name"] = "atlas-actors/" + label
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "2500ms"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "1500ms"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "3000ms"
	cfg.ConnConfig.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, CancelRequestDelay: 25 * time.Millisecond, DeadlineDelay: 750 * time.Millisecond}
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}
func (d *Database) Close() error {
	if d.Pool != nil {
		d.Pool.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, e := d.admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{d.Name}.Sanitize()+" WITH (FORCE)")
	d.admin.Close()
	if e != nil {
		return errors.New("could not drop owned actor database")
	}
	return nil
}
