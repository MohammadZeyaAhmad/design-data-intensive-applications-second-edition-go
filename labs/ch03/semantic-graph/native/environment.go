// Package native compares real PostgreSQL and Fuseki with the Go models.
package native

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch03/semantic-graph/sparql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"time"
)

type Environment struct {
	Pool     *pgxpool.Pool
	RDF      *sparql.Client
	admin    *pgxpool.Pool
	database string
}

func ID() (string, error) {
	var b [16]byte
	_, e := rand.Read(b[:])
	return hex.EncodeToString(b[:]), e
}

func Open(ctx context.Context) (*Environment, error) {
	dsn := os.Getenv("ATLAS_RDF_POSTGRES_URL")
	u, e := url.Parse(dsn)
	if e != nil || u.Path != "/atlas_test" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") || (u.Scheme != "postgresql" && u.Scheme != "postgres") || u.Fragment != "" || u.RawQuery != "sslmode=disable" {
		// pgx accepts query-string host/service overrides. Checking Hostname
		// alone would not establish that the effective connection is local.
		return nil, errors.New("semantic PostgreSQL binding must identify loopback atlas_test without connection overrides")
	}
	client, e := sparql.NewClient(os.Getenv("ATLAS_RDF_URL"), os.Getenv("ATLAS_RDF_PASSWORD"))
	if e != nil {
		return nil, e
	}
	if ok, e := client.Ask(ctx, "ASK{}"); e != nil || !ok {
		return nil, errors.New("authenticated RDF profile is not ready")
	}
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		return nil, errors.New("could not initialize semantic test administrator")
	}
	id, e := ID()
	if e != nil {
		admin.Close()
		return nil, e
	}
	env := &Environment{admin: admin, database: "atlas_semantic_run_" + id, RDF: client}
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{env.database}.Sanitize()); e != nil {
		admin.Close()
		return nil, errors.New("could not create owned semantic test database")
	}
	ok := false
	defer func() {
		if !ok {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			env.Close(cleanup)
		}
	}()
	u.Path = "/" + env.database
	cfg, e := pgxpool.ParseConfig(u.String())
	if e != nil {
		return nil, errors.New("invalid semantic database configuration")
	}
	cfg.MaxConns = 8
	env.Pool, e = pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	if e = env.Pool.Ping(ctx); e != nil {
		return nil, e
	}
	ok = true
	return env, nil
}

func (e *Environment) Close(ctx context.Context) error {
	if e.Pool != nil {
		e.Pool.Close()
	}
	if e.admin == nil {
		return nil
	}
	_, err := e.admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{e.database}.Sanitize()+" WITH(FORCE)")
	e.admin.Close()
	e.admin = nil
	return err
}
