package logformats

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type ConsumerOptions struct {
	SourceDSN, TargetDSN string
	Identity             Identity
	Marker               string
	ReplayRetained       bool
	Hook                 func(string, Transaction) error
}
type ConsumerReport struct {
	Transactions   int        `json:"transactions_received"`
	Duplicates     int        `json:"transactions_deduplicated"`
	Changes        int        `json:"row_changes_received"`
	ToastUnchanged int        `json:"unchanged_toast_cells"`
	Last           Checkpoint `json:"last_durable_checkpoint"`
	Received       Position   `json:"last_received_wal_start"`
	Keepalive      Position   `json:"last_keepalive_wal_end"`
	Acknowledged   Position   `json:"acknowledged_lsn"`
	Operations     []string   `json:"operations"`
}

func replicationConnection(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	c, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid logical source configuration")
	}
	c.RuntimeParams["replication"] = "database"
	c.RuntimeParams["application_name"] = "atlas-cdc-source"
	c.RuntimeParams["timezone"] = "UTC"
	c.RuntimeParams["datestyle"] = "ISO,YMD"
	conn, err := pgconn.ConnectConfig(ctx, c)
	if err != nil {
		return nil, errors.New("logical source unavailable")
	}
	return conn, nil
}
func ValidateSource(ctx context.Context, dsn string, identity Identity, checkpoint Checkpoint) (Position, error) {
	if !safeName.MatchString(identity.Slot) || !safeName.MatchString(identity.Publication) || checkpoint.Identity != identity {
		return 0, ErrSource
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, errors.New("logical source validation unavailable")
	}
	defer conn.Close(context.Background())
	var epoch, plugin, confirmed, status string
	err = conn.QueryRow(ctx, `SELECT i.epoch,s.plugin,coalesce(s.confirmed_flush_lsn,'0/0')::text,coalesce(s.wal_status,'unknown')
FROM log_lab.stream_identity i JOIN pg_replication_slots s ON s.slot_name=i.slot WHERE i.slot=$1`, identity.Slot).Scan(&epoch, &plugin, &confirmed, &status)
	if err != nil || epoch != identity.Epoch || plugin != "pgoutput" || status == "lost" {
		return 0, ErrSource
	}
	position, err := ParsePosition(confirmed)
	if err != nil || position > checkpoint.LSN {
		return 0, ErrSource
	}
	return position, nil
}
func Consume(ctx context.Context, o ConsumerOptions) (ConsumerReport, error) {
	r := ConsumerReport{Operations: []string{}}
	sink, err := NewSink(ctx, o.TargetDSN, o.Identity)
	if err != nil {
		return r, err
	}
	defer sink.Close()
	sink.Hook = o.Hook
	checkpoint, err := sink.Checkpoint(ctx)
	if err != nil {
		return r, err
	}
	confirmed, err := ValidateSource(ctx, o.SourceDSN, o.Identity, checkpoint)
	if err != nil {
		return r, err
	}
	conn, err := replicationConnection(ctx, o.SourceDSN)
	if err != nil {
		return r, err
	}
	defer conn.Close(context.Background())
	system, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil || system.SystemID != o.Identity.SystemID || system.DBName != o.Identity.Database {
		return r, ErrSource
	}
	start := checkpoint.LSN
	if o.ReplayRetained {
		start = confirmed
	}
	if err = pglogrepl.StartReplication(ctx, conn, o.Identity.Slot, pglogrepl.LSN(start), pglogrepl.StartReplicationOptions{PluginArgs: []string{"proto_version '1'", "publication_names '" + o.Identity.Publication + "'"}}); err != nil {
		return r, errors.New("logical streaming could not start")
	}
	decoder := NewDecoder()
	r.Last = checkpoint
	nextStatus := time.Now()
	ack := func() error {
		position := pglogrepl.LSN(r.Last.LSN)
		err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: position, WALFlushPosition: position, WALApplyPosition: position})
		if err == nil {
			r.Acknowledged = r.Last.LSN
			nextStatus = time.Now().Add(time.Second)
		}
		return err
	}
	for {
		if err = ctx.Err(); err != nil {
			return r, err
		}
		if time.Now().After(nextStatus) {
			if err = ack(); err != nil {
				return r, err
			}
		}
		readCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		message, err := conn.ReceiveMessage(readCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			return r, errors.New("logical stream interrupted")
		}
		data, ok := message.(*pgproto3.CopyData)
		if !ok {
			if _, failed := message.(*pgproto3.ErrorResponse); failed {
				return r, errors.New("logical source rejected stream")
			}
			continue
		}
		if len(data.Data) == 0 {
			return r, ErrProtocol
		}
		switch data.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(data.Data[1:])
			if err != nil {
				return r, ErrProtocol
			}
			r.Keepalive = Position(keepalive.ServerWALEnd)
			if keepalive.ReplyRequested {
				if err = ack(); err != nil {
					return r, err
				}
			}
		case pglogrepl.XLogDataByteID:
			packet, err := pglogrepl.ParseXLogData(data.Data[1:])
			if err != nil {
				return r, ErrProtocol
			}
			r.Received = Position(packet.WALStart)
			batch, err := decoder.Feed(packet.WALData)
			if err != nil {
				return r, err
			}
			if batch == nil {
				continue
			}
			r.Transactions++
			r.Changes += len(batch.Changes)
			for _, change := range batch.Changes {
				r.Operations = append(r.Operations, change.Kind+":"+change.Relation.Name)
				for _, cell := range change.New {
					if cell.Kind == 'u' {
						r.ToastUnchanged++
					}
				}
			}
			duplicate, err := sink.Apply(ctx, *batch)
			if err != nil {
				return r, err
			}
			if duplicate {
				r.Duplicates++
			}
			r.Last, err = sink.Checkpoint(ctx)
			if err != nil {
				return r, err
			}
			if err = ack(); err != nil {
				return r, err
			}
			found, err := sink.HasMarker(ctx, o.Marker)
			if err != nil {
				return r, err
			}
			if found {
				return r, nil
			}
		default:
			return r, ErrProtocol
		}
	}
}

// ExportBootstrap keeps the exporting connection idle until the SQL transaction
// imports its snapshot. A concurrent source writer can then commit while the
// exported snapshot remains stable and the slot retains its later changes.
func ExportBootstrap(ctx context.Context, sourceDSN string, sink *Sink, identity Identity, afterImport func() error) (Checkpoint, error) {
	conn, err := replicationConnection(ctx, sourceDSN)
	if err != nil {
		return Checkpoint{}, err
	}
	defer conn.Close(context.Background())
	system, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil || system.SystemID != identity.SystemID || system.DBName != identity.Database {
		return Checkpoint{}, ErrSource
	}
	slot, err := pglogrepl.CreateReplicationSlot(ctx, conn, identity.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return Checkpoint{}, errors.New("logical snapshot export failed")
	}
	position, err := ParsePosition(slot.ConsistentPoint)
	if err != nil || slot.SnapshotName == "" {
		return Checkpoint{}, ErrProtocol
	}
	sql, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		return Checkpoint{}, errors.New("snapshot source unavailable")
	}
	defer sql.Close(context.Background())
	tx, err := sql.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Checkpoint{}, err
	}
	defer tx.Rollback(context.Background())
	// Snapshot identifiers come from PostgreSQL and are quoted as SQL literals.
	if _, err = tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+literal(slot.SnapshotName)); err != nil {
		return Checkpoint{}, err
	}
	if afterImport != nil {
		if err = afterImport(); err != nil {
			return Checkpoint{}, err
		}
	}
	if err = sink.Bootstrap(ctx, tx, position); err != nil {
		return Checkpoint{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Checkpoint{}, err
	}
	return Checkpoint{identity, position}, nil
}
func literal(s string) string {
	out := "'"
	for _, c := range s {
		if c == '\'' {
			out += "''"
		} else {
			out += string(c)
		}
	}
	return out + "'"
}
