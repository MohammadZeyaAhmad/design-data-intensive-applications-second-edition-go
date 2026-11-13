// Package durable demonstrates a local transactional receipt, separate from
// actor scheduling and from the broker's consumer offset.
package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/actors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Schema = `
CREATE SCHEMA actors;
CREATE TABLE actors.stock (
 subscription text NOT NULL,tenant text NOT NULL,warehouse text NOT NULL,sku text NOT NULL,
 on_hand bigint NOT NULL CHECK(on_hand>=0),reserved bigint NOT NULL CHECK(reserved>=0 AND reserved<=on_hand),version bigint NOT NULL CHECK(version>=0),
 PRIMARY KEY(subscription,tenant,warehouse,sku)
);
CREATE TABLE actors.receipts (
 subscription text NOT NULL,tenant text NOT NULL,warehouse text NOT NULL,sku text NOT NULL,message_id text NOT NULL,
 body_hash bytea NOT NULL CHECK(octet_length(body_hash)=32),source_body bytea NOT NULL CHECK(octet_length(source_body)<=4096),response jsonb NOT NULL,
 PRIMARY KEY(subscription,tenant,warehouse,sku,message_id),
 FOREIGN KEY(subscription,tenant,warehouse,sku) REFERENCES actors.stock(subscription,tenant,warehouse,sku)
);
CREATE TABLE actors.attempts (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,subscription text NOT NULL,message_id text NOT NULL,worker text NOT NULL,
 topic text NOT NULL,partition_id integer NOT NULL,offset_id bigint NOT NULL,replayed boolean NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);`

type Source struct {
	Topic     string
	Partition int32
	Offset    int64
}
type Store struct {
	Pool         *pgxpool.Pool
	BeforeCommit func(context.Context, actors.Message) error
}

var subscriptionPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,95}$`)

func (s Store) Apply(parent context.Context, subscription, worker string, m actors.Message, source Source) (out actors.Reply, resultErr error) {
	if !subscriptionPattern.MatchString(subscription) || !subscriptionPattern.MatchString(worker) || !m.Key.Valid() {
		return out, actors.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	defer func() {
		if resultErr != nil && ctx.Err() != nil {
			resultErr = ctx.Err()
		}
	}()
	if m.Kind == "get.v1" {
		state, e := s.State(ctx, subscription, m.Key)
		return actors.Reply{State: state}, e
	}
	tx, e := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		return out, e
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	key := m.Key.Normalize()
	args := []any{subscription, key.Tenant, key.Warehouse, key.SKU}
	if _, e = tx.Exec(ctx, `INSERT INTO actors.stock(subscription,tenant,warehouse,sku,on_hand,reserved,version) VALUES($1,$2,$3,$4,0,0,0) ON CONFLICT DO NOTHING`, args...); e != nil {
		return out, e
	}
	state := actors.State{Key: key}
	if e = tx.QueryRow(ctx, `SELECT on_hand,reserved,version FROM actors.stock WHERE subscription=$1 AND tenant=$2 AND warehouse=$3 AND sku=$4 FOR UPDATE`, args...).Scan(&state.OnHand, &state.Reserved, &state.Version); e != nil {
		return out, e
	}
	var hash, response []byte
	e = tx.QueryRow(ctx, `SELECT body_hash,response FROM actors.receipts WHERE subscription=$1 AND tenant=$2 AND warehouse=$3 AND sku=$4 AND message_id=$5`, subscription, key.Tenant, key.Warehouse, key.SKU, m.ID).Scan(&hash, &response)
	switch {
	case e == nil:
		if !bytes.Equal(hash, m.Hash[:]) {
			return out, actors.ErrIdentityConflict
		}
		if e = json.Unmarshal(response, &out); e != nil {
			return out, e
		}
		out.Replayed = true
	case errors.Is(e, pgx.ErrNoRows):
		next, e := actors.Evolve(state, m)
		if e != nil {
			return out, e
		}
		out = actors.Reply{State: next, Note: m.Note}
		response, e = json.Marshal(out)
		if e != nil {
			return out, e
		}
		if _, e = tx.Exec(ctx, `UPDATE actors.stock SET on_hand=$5,reserved=$6,version=$7 WHERE subscription=$1 AND tenant=$2 AND warehouse=$3 AND sku=$4`, subscription, key.Tenant, key.Warehouse, key.SKU, next.OnHand, next.Reserved, next.Version); e != nil {
			return out, e
		}
		if _, e = tx.Exec(ctx, `INSERT INTO actors.receipts(subscription,tenant,warehouse,sku,message_id,body_hash,source_body,response) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, subscription, key.Tenant, key.Warehouse, key.SKU, m.ID, m.Hash[:], m.Raw, response); e != nil {
			return out, e
		}
	default:
		return out, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO actors.attempts(subscription,message_id,worker,topic,partition_id,offset_id,replayed) VALUES($1,$2,$3,$4,$5,$6,$7)`, subscription, m.ID, worker, source.Topic, source.Partition, source.Offset, out.Replayed); e != nil {
		return out, e
	}
	if s.BeforeCommit != nil {
		if e = s.BeforeCommit(ctx, m); e != nil {
			return out, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return out, e
	}
	return out, nil
}
func (s Store) State(ctx context.Context, subscription string, key actors.Key) (actors.State, error) {
	key = key.Normalize()
	out := actors.State{Key: key}
	e := s.Pool.QueryRow(ctx, `SELECT on_hand,reserved,version FROM actors.stock WHERE subscription=$1 AND tenant=$2 AND warehouse=$3 AND sku=$4`, subscription, key.Tenant, key.Warehouse, key.SKU).Scan(&out.OnHand, &out.Reserved, &out.Version)
	if errors.Is(e, pgx.ErrNoRows) {
		e = nil
	}
	return out, e
}
