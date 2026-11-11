package graphnative

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Environment struct {
	Pool     *pgxpool.Pool
	Driver   neo4j.DriverWithContext
	admin    *pgxpool.Pool
	database string
}

func ID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	return hex.EncodeToString(b[:]), nil
}
func Open(ctx context.Context) (*Environment, error) {
	dsn := os.Getenv("ATLAS_GRAPH_POSTGRES_URL")
	u, e := url.Parse(dsn)
	if e != nil || u.Path != "/atlas_test" || u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" || u.Scheme != "postgresql" && u.Scheme != "postgres" {
		return nil, errors.New("graph PostgreSQL binding must identify loopback atlas_test")
	}
	boltURI := os.Getenv("ATLAS_GRAPH_NEO4J_URI")
	bolt, e := url.Parse(boltURI)
	if e != nil || bolt.Scheme != "bolt" || bolt.User != nil || bolt.RawQuery != "" || bolt.Path != "" || bolt.Hostname() != "127.0.0.1" && bolt.Hostname() != "localhost" {
		return nil, errors.New("graph Neo4j binding must be a loopback bolt URI without credentials")
	}
	password := os.Getenv("ATLAS_GRAPH_PASSWORD")
	if len(password) < 12 {
		return nil, errors.New("configure the graph profile credentials first")
	}
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		return nil, errors.New("could not initialize graph test administrator")
	}
	id, e := ID()
	if e != nil {
		admin.Close()
		return nil, e
	}
	env := &Environment{admin: admin, database: "atlas_graph_run_" + id}
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{env.database}.Sanitize()); e != nil {
		admin.Close()
		return nil, errors.New("could not create owned graph test database")
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
		return nil, errors.New("invalid graph test configuration")
	}
	cfg.MaxConns = 8
	env.Pool, e = pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	env.Driver, e = neo4j.NewDriverWithContext(boltURI, neo4j.BasicAuth("neo4j", password, ""), func(c *neo4j.Config) {
		c.SocketConnectTimeout = 5 * time.Second
		c.ConnectionAcquisitionTimeout = 5 * time.Second
		c.MaxTransactionRetryTime = 5 * time.Second
	})
	if e != nil {
		return nil, e
	}
	if e = env.Driver.VerifyConnectivity(ctx); e != nil {
		return nil, fmt.Errorf("Neo4j graph profile is not ready: %w", e)
	}
	ok = true
	return env, nil
}
func (e *Environment) Close(ctx context.Context) error {
	var result error
	if e.Driver != nil {
		result = errors.Join(result, e.Driver.Close(ctx))
	}
	if e.Pool != nil {
		e.Pool.Close()
	}
	if e.admin != nil {
		_, err := e.admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{e.database}.Sanitize()+" WITH(FORCE)")
		result = errors.Join(result, err)
		e.admin.Close()
		e.admin = nil
	}
	return result
}
