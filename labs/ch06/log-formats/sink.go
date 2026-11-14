package logformats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var tables = []string{"stock", "receipts", "scratch", "barriers"}
var sqlTypes = map[uint32]string{16: "boolean", 20: "bigint", 21: "smallint", 23: "integer", 25: "text", 1082: "date", 1184: "timestamp with time zone"}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func allowed(r Relation) bool {
	if r.Namespace != "log_lab" {
		return false
	}
	for _, table := range tables {
		if table == r.Name {
			return true
		}
	}
	return false
}
func tableSQL(r Relation) string { return pgx.Identifier{r.Namespace, r.Name}.Sanitize() }
func Catalog(ctx context.Context, q queryer, table string) (Relation, error) {
	if !safeName.MatchString(table) {
		return Relation{}, ErrSchema
	}
	rows, err := q.Query(ctx, `SELECT c.oid,c.relreplident::text,a.attname,a.atttypid,a.atttypmod,
coalesce(a.attnum=ANY(i.indkey::smallint[]),false)
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_index i ON i.indrelid=c.oid AND i.indisprimary
WHERE n.nspname='log_lab' AND c.relname=$1 ORDER BY a.attnum`, table)
	if err != nil {
		return Relation{}, err
	}
	defer rows.Close()
	r := Relation{Namespace: "log_lab", Name: table}
	for rows.Next() {
		var column Column
		var identity string
		if err = rows.Scan(&r.ID, &identity, &column.Name, &column.OID, &column.Modifier, &column.Key); err != nil {
			return r, err
		}
		if len(identity) != 1 {
			return r, ErrSchema
		}
		r.Identity = identity[0]
		r.Columns = append(r.Columns, column)
	}
	if err = rows.Err(); err != nil {
		return r, err
	}
	if len(r.Columns) == 0 || len(r.Columns) > MaxColumns {
		return r, ErrSchema
	}
	return r, nil
}
func match(source, target Relation) error {
	if !allowed(source) || source.Name != target.Name || source.Namespace != target.Namespace || len(source.Columns) != len(target.Columns) {
		return ErrSchema
	}
	keys := 0
	for i, s := range source.Columns {
		t := target.Columns[i]
		if s.Name != t.Name || s.OID != t.OID || s.Modifier != t.Modifier || sqlTypes[t.OID] == "" || (t.Key && !s.Key) {
			return ErrSchema
		}
		if t.Key {
			keys++
		}
	}
	if keys == 0 {
		return ErrSchema
	}
	return nil
}

type Sink struct {
	Pool     *pgxpool.Pool
	Identity Identity
	Hook     func(string, Transaction) error
}

func NewSink(ctx context.Context, dsn string, identity Identity) (*Sink, error) {
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid logical sink configuration")
	}
	c.MaxConns = 4
	c.ConnConfig.RuntimeParams["application_name"] = "atlas-cdc-sink"
	c.ConnConfig.RuntimeParams["timezone"] = "UTC"
	c.ConnConfig.RuntimeParams["statement_timeout"] = "5000ms"
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "3000ms"
	pool, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, errors.New("logical sink unavailable")
	}
	return &Sink{Pool: pool, Identity: identity}, nil
}
func (s *Sink) Close() { s.Pool.Close() }
func (s *Sink) Init(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS sink;
CREATE TABLE IF NOT EXISTS sink.checkpoints(stream text PRIMARY KEY, identity jsonb NOT NULL, lsn pg_lsn NOT NULL);
CREATE TABLE IF NOT EXISTS sink.transactions(stream text NOT NULL,end_lsn pg_lsn NOT NULL,xid bigint NOT NULL,changes integer NOT NULL,PRIMARY KEY(stream,end_lsn));
CREATE TABLE IF NOT EXISTS sink.changes(stream text NOT NULL,end_lsn pg_lsn NOT NULL,ordinal integer NOT NULL,relation text NOT NULL,operation text NOT NULL,PRIMARY KEY(stream,end_lsn,ordinal));`)
	return err
}
func (s *Sink) checkpoint(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, lock bool) (Checkpoint, error) {
	sql := "SELECT identity::text,lsn::text FROM sink.checkpoints WHERE stream=$1"
	if lock {
		sql += " FOR UPDATE"
	}
	var body, lsn string
	if err := q.QueryRow(ctx, sql, s.Identity.Slot).Scan(&body, &lsn); err != nil {
		return Checkpoint{}, err
	}
	var identity Identity
	if json.Unmarshal([]byte(body), &identity) != nil || identity != s.Identity {
		return Checkpoint{}, ErrSource
	}
	position, err := ParsePosition(lsn)
	return Checkpoint{identity, position}, err
}
func (s *Sink) Checkpoint(ctx context.Context) (Checkpoint, error) {
	return s.checkpoint(ctx, s.Pool, false)
}
func rowInsert(ctx context.Context, tx pgx.Tx, r Relation, cells []Cell) error {
	if len(cells) != len(r.Columns) {
		return ErrProtocol
	}
	columns, values := []string{}, []string{}
	args := []any{}
	for i, column := range r.Columns {
		columns = append(columns, pgx.Identifier{column.Name}.Sanitize())
		values = append(values, "$"+strconv.Itoa(i+1)+"::text::"+sqlTypes[column.OID])
		if cells[i].Kind == 'n' {
			args = append(args, nil)
		} else if cells[i].Kind == 't' {
			args = append(args, cells[i].Text)
		} else {
			return ErrProjection
		}
	}
	_, err := tx.Exec(ctx, "INSERT INTO "+tableSQL(r)+"("+strings.Join(columns, ",")+") VALUES("+strings.Join(values, ",")+")", args...)
	return err
}
func whereKeys(r Relation, cells []Cell, args *[]any) (string, error) {
	if len(cells) != len(r.Columns) {
		return "", ErrProjection
	}
	predicates := []string{}
	for i, column := range r.Columns {
		if !column.Key {
			continue
		}
		if cells[i].Kind != 't' {
			return "", ErrProjection
		}
		*args = append(*args, cells[i].Text)
		predicates = append(predicates, pgx.Identifier{column.Name}.Sanitize()+"=$"+strconv.Itoa(len(*args))+"::text::"+sqlTypes[column.OID])
	}
	if len(predicates) == 0 {
		return "", ErrSchema
	}
	return strings.Join(predicates, " AND "), nil
}
func applyChange(ctx context.Context, tx pgx.Tx, change Change, target Relation) error {
	if err := match(change.Relation, target); err != nil {
		return err
	}
	switch change.Kind {
	case "insert":
		return rowInsert(ctx, tx, target, change.New)
	case "update":
		if len(change.New) != len(target.Columns) {
			return ErrProtocol
		}
		sets := []string{}
		args := []any{}
		for i, column := range target.Columns {
			cell := change.New[i]
			if cell.Kind == 'u' {
				continue
			}
			if cell.Kind == 'n' {
				args = append(args, nil)
			} else if cell.Kind == 't' {
				args = append(args, cell.Text)
			} else {
				return ErrProtocol
			}
			sets = append(sets, pgx.Identifier{column.Name}.Sanitize()+"=$"+strconv.Itoa(len(args))+"::text::"+sqlTypes[column.OID])
		}
		if len(sets) == 0 {
			return ErrProjection
		}
		key := change.Old
		if len(key) == 0 {
			key = change.New
		}
		where, err := whereKeys(target, key, &args)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, "UPDATE "+tableSQL(target)+" SET "+strings.Join(sets, ",")+" WHERE "+where, args...)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrProjection
		}
		return nil
	case "delete":
		args := []any{}
		where, err := whereKeys(target, change.Old, &args)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, "DELETE FROM "+tableSQL(target)+" WHERE "+where, args...)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrProjection
		}
		return nil
	case "truncate":
		_, err := tx.Exec(ctx, "TRUNCATE "+tableSQL(target))
		return err
	}
	return ErrProtocol
}
func (s *Sink) Apply(ctx context.Context, batch Transaction) (bool, error) {
	if batch.End <= batch.Commit || batch.Commit != batch.Begin || len(batch.Changes) > MaxChanges {
		return false, ErrProtocol
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	checkpoint, err := s.checkpoint(ctx, tx, true)
	if err != nil {
		return false, err
	}
	if batch.End <= checkpoint.LSN {
		return true, tx.Commit(ctx)
	}
	for i, change := range batch.Changes {
		target, err := Catalog(ctx, tx, change.Relation.Name)
		if err != nil {
			return false, err
		}
		if err = applyChange(ctx, tx, change, target); err != nil {
			return false, err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO sink.changes(stream,end_lsn,ordinal,relation,operation) VALUES($1,$2::pg_lsn,$3,$4,$5)", s.Identity.Slot, batch.End.String(), i+1, change.Relation.Qualified(), change.Kind); err != nil {
			return false, err
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO sink.transactions(stream,end_lsn,xid,changes) VALUES($1,$2::pg_lsn,$3,$4)", s.Identity.Slot, batch.End.String(), int64(batch.XID), len(batch.Changes)); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE sink.checkpoints SET lsn=$2::pg_lsn WHERE stream=$1", s.Identity.Slot, batch.End.String()); err != nil {
		return false, err
	}
	if s.Hook != nil {
		if err = s.Hook("before_commit", batch); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	if s.Hook != nil {
		if err = s.Hook("after_commit", batch); err != nil {
			return false, err
		}
	}
	return false, nil
}
func (s *Sink) HasMarker(ctx context.Context, marker string) (bool, error) {
	var found bool
	err := s.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM log_lab.barriers WHERE id=$1)", marker).Scan(&found)
	return found, err
}
func (s *Sink) Bootstrap(ctx context.Context, source pgx.Tx, position Position) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "TRUNCATE log_lab.stock,log_lab.receipts,log_lab.scratch,log_lab.barriers"); err != nil {
		return err
	}
	for _, table := range tables {
		r, err := Catalog(ctx, source, table)
		if err != nil {
			return err
		}
		target, err := Catalog(ctx, tx, table)
		if err != nil {
			return err
		}
		if err = match(r, target); err != nil {
			return err
		}
		columns, keys := []string{}, []string{}
		for _, column := range r.Columns {
			name := pgx.Identifier{column.Name}.Sanitize()
			columns = append(columns, name+"::text")
			if column.Key {
				keys = append(keys, name)
			}
		}
		rows, err := source.Query(ctx, "SELECT "+strings.Join(columns, ",")+" FROM "+tableSQL(r)+" ORDER BY "+strings.Join(keys, ","))
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			count++
			if count > 4096 {
				rows.Close()
				return ErrLimit
			}
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return err
			}
			cells := make([]Cell, len(values))
			for i, value := range values {
				if value == nil {
					cells[i] = Cell{Kind: 'n'}
				} else if text, ok := value.(string); ok {
					cells[i] = Cell{Kind: 't', Text: text}
				} else {
					rows.Close()
					return ErrProtocol
				}
			}
			if err = rowInsert(ctx, tx, target, cells); err != nil {
				rows.Close()
				return err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	body, _ := json.Marshal(s.Identity)
	if _, err = tx.Exec(ctx, "INSERT INTO sink.checkpoints(stream,identity,lsn) VALUES($1,$2::jsonb,$3::pg_lsn)", s.Identity.Slot, string(body), position.String()); err != nil {
		return fmt.Errorf("bootstrap checkpoint: %w", err)
	}
	return tx.Commit(ctx)
}
