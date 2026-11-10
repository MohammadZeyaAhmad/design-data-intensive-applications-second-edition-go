package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/cmdtx"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/operator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrOffsetConflict = errors.New("broker offset identity changed")
var ErrConflict = errors.New("inbox control conflict")

type Inbox struct {
	Pool  *pgxpool.Pool
	Group string
}

func (s Inbox) Stage(ctx context.Context, r api.Record) error {
	if s.Group == "" || len(s.Group) > 128 || !api.TopicValid(r.Topic) || r.Partition < 0 || r.Offset < 0 {
		return api.ErrInvalid
	}
	h := sha256.New()
	_ = binary.Write(h, binary.BigEndian, uint64(len(r.Key)))
	_, _ = h.Write(r.Key)
	_, _ = h.Write(r.Value)
	rawHash := h.Sum(nil)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer cmdtx.Rollback(tx)
	tag, err := tx.Exec(ctx, `INSERT INTO messaging.deliveries(group_id,topic,partition,offset_id,body_hash) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, s.Group, r.Topic, r.Partition, r.Offset, rawHash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var old []byte
		if err = tx.QueryRow(ctx, `SELECT body_hash FROM messaging.deliveries WHERE group_id=$1 AND topic=$2 AND partition=$3 AND offset_id=$4`, s.Group, r.Topic, r.Partition, r.Offset).Scan(&old); err != nil {
			return err
		}
		if !bytes.Equal(old, rawHash) {
			return ErrOffsetConflict
		}
		return tx.Commit(ctx)
	}
	reject := func(reason string) error {
		key, value := r.Key, r.Value
		if len(key) > 1024 {
			key = key[:1024]
		}
		if len(value) > api.MaxBytes*2 {
			value = value[:api.MaxBytes*2]
		}
		_, err := tx.Exec(ctx, `INSERT INTO messaging.rejected(group_id,topic,partition,offset_id,record_key,record_value,reason) VALUES($1,$2,$3,$4,$5,$6,$7)`, s.Group, r.Topic, r.Partition, r.Offset, key, value, reason)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	m, err := api.Decode(r.Topic, r.Value)
	if err != nil {
		return reject("invalid_envelope")
	}
	if !bytes.Equal(m.Key(), r.Key) || r.Partition != api.Partition(r.Key) {
		return reject("partition_key_mismatch")
	}
	canonical, err := api.Canonical(r.Value)
	if err != nil {
		return reject("invalid_envelope")
	}
	hash := sha256.Sum256(canonical)
	tag, err = tx.Exec(ctx, `INSERT INTO messaging.inbox(topic,message_id,tenant_id,stream_id,sequence,envelope,body_hash) VALUES($1,$2::uuid,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, r.Topic, m.ID, m.TenantID, m.StreamID, m.Sequence, canonical, hash[:])
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		rows, err := tx.Query(ctx, `SELECT message_id::text,body_hash FROM messaging.inbox WHERE topic=$1 AND (message_id=$2::uuid OR (tenant_id=$3 AND stream_id=$4 AND sequence=$5)) FOR UPDATE`, r.Topic, m.ID, m.TenantID, m.StreamID, m.Sequence)
		if err != nil {
			return err
		}
		same := true
		for rows.Next() {
			var id string
			var old []byte
			if err = rows.Scan(&id, &old); err != nil {
				rows.Close()
				return err
			}
			same = same && id == m.ID && bytes.Equal(old, hash[:])
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if !same {
			if _, err = tx.Exec(ctx, `UPDATE messaging.inbox SET status='quarantined',last_error='identity_conflict' WHERE topic=$1 AND (message_id=$2::uuid OR (tenant_id=$3 AND stream_id=$4 AND sequence=$5))`, r.Topic, m.ID, m.TenantID, m.StreamID, m.Sequence); err != nil {
				return err
			}
			return reject("identity_conflict")
		}
	}
	return tx.Commit(ctx)
}

func (s Inbox) ProcessOne(ctx context.Context, h api.Handler) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer cmdtx.Rollback(tx)
	var topic, id string
	var body []byte
	err = tx.QueryRow(ctx, `SELECT i.topic,i.message_id::text,i.envelope FROM messaging.inbox i WHERE i.status='pending' AND i.available_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM messaging.inbox earlier WHERE earlier.topic=i.topic AND earlier.tenant_id=i.tenant_id AND earlier.stream_id=i.stream_id AND earlier.sequence<i.sequence AND earlier.status<>'done') ORDER BY i.available_at,i.topic,i.tenant_id,i.stream_id,i.sequence FOR UPDATE OF i SKIP LOCKED LIMIT 1`).Scan(&topic, &id, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	m, err := api.Decode(topic, body)
	if err == nil {
		err = h.Apply(ctx, topic, m)
	}
	if err != nil {
		cmdtx.Rollback(tx)
		if ctx.Err() == nil {
			_, updateErr := s.Pool.Exec(ctx, `UPDATE messaging.inbox SET attempts=attempts+1,cycle_attempts=cycle_attempts+1,status=CASE WHEN cycle_attempts+1>=5 THEN 'quarantined' ELSE status END,available_at=clock_timestamp()+interval '500 milliseconds',last_error='handler_retry' WHERE topic=$1 AND message_id=$2::uuid AND status='pending'`, topic, id)
			if updateErr != nil {
				return true, errors.Join(err, updateErr)
			}
		}
		return true, err
	}
	if _, err = tx.Exec(ctx, `UPDATE messaging.inbox SET status='done',applied_at=coalesce(applied_at,clock_timestamp()),last_error=NULL WHERE topic=$1 AND message_id=$2::uuid`, topic, id); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
func (s Inbox) Run(ctx context.Context, h api.Handler, onError func(error)) error {
	for ctx.Err() == nil {
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		did, err := s.ProcessOne(step, h)
		cancel()
		if err != nil && ctx.Err() == nil {
			onError(err)
		}
		if !did || err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return ctx.Err()
}

type Retry struct {
	TenantID, Topic, ID, Key, Reason string
	ExpectedVersion                  int64
}
type RetryResult struct {
	ID      string `json:"id"`
	Version int64  `json:"control_version"`
	Status  string `json:"status"`
}

func (s Inbox) Retry(ctx context.Context, c Retry) (RetryResult, bool, error) {
	if err := operator.Require(ctx, c.TenantID); err != nil {
		return RetryResult{}, false, err
	}
	if !api.TopicValid(c.Topic) || !envelope.ValidID(c.ID) || c.ExpectedVersion < 0 || (c.Reason != "consumer_upgraded" && c.Reason != "source_verified") {
		return RetryResult{}, false, api.ErrInvalid
	}
	payload := struct {
		Tenant, Topic, ID, Reason string
		Version                   int64
	}{c.TenantID, c.Topic, c.ID, c.Reason, c.ExpectedVersion}
	r, err := commands.New(c.TenantID, "inbox-retry", c.Key, payload)
	if err != nil {
		return RetryResult{}, false, err
	}
	return cmdtx.Execute(ctx, s.Pool, "messaging", r, func(tx pgx.Tx) (RetryResult, error) {
		var version int64
		var status string
		err := tx.QueryRow(ctx, `SELECT control_version,status FROM messaging.inbox WHERE tenant_id=$1 AND topic=$2 AND message_id=$3::uuid FOR UPDATE`, c.TenantID, c.Topic, c.ID).Scan(&version, &status)
		if err != nil {
			return RetryResult{}, err
		}
		if status != "quarantined" || version != c.ExpectedVersion {
			return RetryResult{}, ErrConflict
		}
		m, err := envelope.Record(ctx, "operator:local", c.ID)
		if err != nil {
			return RetryResult{}, err
		}
		m.Actor = "operator:local"
		meta, _ := json.Marshal(m)
		if _, err = tx.Exec(ctx, `UPDATE messaging.inbox SET status='pending',cycle_attempts=0,control_version=control_version+1,available_at=clock_timestamp() WHERE tenant_id=$1 AND topic=$2 AND message_id=$3::uuid`, c.TenantID, c.Topic, c.ID); err != nil {
			return RetryResult{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO messaging.retry_audit(tenant_id,topic,message_id,version,reason,metadata) VALUES($1,$2,$3::uuid,$4,$5,$6)`, c.TenantID, c.Topic, c.ID, version+1, c.Reason, meta)
		return RetryResult{ID: c.ID, Version: version + 1, Status: "pending"}, err
	})
}

type Stats struct{ Pending, Done, Quarantined, Rejected, Deliveries int64 }

func (s Inbox) Stats(ctx context.Context) (Stats, error) {
	var v Stats
	err := s.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM messaging.inbox WHERE status='pending'),(SELECT count(*) FROM messaging.inbox WHERE status='done'),(SELECT count(*) FROM messaging.inbox WHERE status='quarantined'),(SELECT count(*) FROM messaging.rejected),(SELECT count(*) FROM messaging.deliveries)`).Scan(&v.Pending, &v.Done, &v.Quarantined, &v.Rejected, &v.Deliveries)
	return v, err
}
