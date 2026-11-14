package logformats

import "github.com/jackc/pglogrepl"

// parse contains malformed-frame panics in the upstream v1 parser. Frames and
// per-transaction accumulation are bounded before invoking that parser.
func parse(frame []byte) (message pglogrepl.Message, err error) {
	if err = preflight(frame); err != nil {
		return nil, err
	}
	defer func() {
		if recover() != nil {
			message = nil
			err = ErrProtocol
		}
	}()
	message, err = pglogrepl.Parse(frame)
	if err != nil {
		return nil, ErrProtocol
	}
	return message, nil
}

type Decoder struct {
	relations map[uint32]Relation
	pending   *Transaction
	bytes     int
	last      Position
}

func NewDecoder() *Decoder { return &Decoder{relations: map[uint32]Relation{}} }
func tuple(t *pglogrepl.TupleData, r Relation, insert bool) ([]Cell, error) {
	if t == nil || int(t.ColumnNum) != len(r.Columns) || len(t.Columns) != len(r.Columns) {
		return nil, ErrProtocol
	}
	cells := make([]Cell, len(t.Columns))
	for i, c := range t.Columns {
		if c == nil {
			return nil, ErrProtocol
		}
		switch c.DataType {
		case 'n':
			cells[i] = Cell{Kind: 'n'}
		case 'u':
			if insert {
				return nil, ErrProtocol
			}
			cells[i] = Cell{Kind: 'u'}
		case 't':
			if len(c.Data) > MaxFrameBytes {
				return nil, ErrLimit
			}
			cells[i] = Cell{Kind: 't', Text: string(c.Data)}
		default:
			return nil, ErrProtocol
		}
	}
	return cells, nil
}
func (d *Decoder) add(c Change) error {
	if d.pending == nil {
		return ErrProtocol
	}
	if len(d.pending.Changes) >= MaxChanges {
		return ErrLimit
	}
	d.pending.Changes = append(d.pending.Changes, c)
	return nil
}
func (d *Decoder) Feed(frame []byte) (*Transaction, error) {
	if d.pending != nil {
		d.bytes += len(frame)
		if d.bytes > MaxTransactionBytes {
			return nil, ErrLimit
		}
	}
	m, err := parse(frame)
	if err != nil {
		return nil, err
	}
	switch m := m.(type) {
	case *pglogrepl.BeginMessage:
		if d.pending != nil || Position(m.FinalLSN) < d.last {
			return nil, ErrProtocol
		}
		d.pending = &Transaction{XID: m.Xid, Begin: Position(m.FinalLSN), Changes: []Change{}}
		d.bytes = len(frame)
	case *pglogrepl.RelationMessage:
		if m.ColumnNum == 0 || int(m.ColumnNum) != len(m.Columns) || len(m.Columns) > MaxColumns || !safeName.MatchString(m.Namespace) || !safeName.MatchString(m.RelationName) || (m.ReplicaIdentity != 'd' && m.ReplicaIdentity != 'f') {
			return nil, ErrSchema
		}
		if _, exists := d.relations[m.RelationID]; !exists && len(d.relations) >= MaxRelations {
			return nil, ErrLimit
		}
		r := Relation{ID: m.RelationID, Namespace: m.Namespace, Name: m.RelationName, Identity: m.ReplicaIdentity}
		seen := map[string]bool{}
		for _, c := range m.Columns {
			if c == nil || !safeName.MatchString(c.Name) || seen[c.Name] || c.Flags > 1 {
				return nil, ErrProtocol
			}
			seen[c.Name] = true
			r.Columns = append(r.Columns, Column{c.Name, c.DataType, c.TypeModifier, c.Flags == 1})
		}
		d.relations[r.ID] = r
	case *pglogrepl.InsertMessage:
		r, ok := d.relations[m.RelationID]
		if !ok {
			return nil, ErrProtocol
		}
		cells, e := tuple(m.Tuple, r, true)
		if e != nil {
			return nil, e
		}
		if err = d.add(Change{Kind: "insert", Relation: r, New: cells}); err != nil {
			return nil, err
		}
	case *pglogrepl.UpdateMessage:
		r, ok := d.relations[m.RelationID]
		if !ok {
			return nil, ErrProtocol
		}
		cells, e := tuple(m.NewTuple, r, false)
		if e != nil {
			return nil, e
		}
		var old []Cell
		if m.OldTuple != nil {
			if m.OldTupleType != 'K' && m.OldTupleType != 'O' {
				return nil, ErrProtocol
			}
			old, e = tuple(m.OldTuple, r, false)
			if e != nil {
				return nil, e
			}
		}
		if err = d.add(Change{Kind: "update", Relation: r, Old: old, New: cells}); err != nil {
			return nil, err
		}
	case *pglogrepl.DeleteMessage:
		r, ok := d.relations[m.RelationID]
		if !ok {
			return nil, ErrProtocol
		}
		if m.OldTupleType != 'K' && m.OldTupleType != 'O' {
			return nil, ErrProtocol
		}
		old, e := tuple(m.OldTuple, r, false)
		if e != nil {
			return nil, e
		}
		if err = d.add(Change{Kind: "delete", Relation: r, Old: old}); err != nil {
			return nil, err
		}
	case *pglogrepl.TruncateMessage:
		if m.Option != 0 || int(m.RelationNum) != len(m.RelationIDs) || len(m.RelationIDs) > MaxRelations {
			return nil, ErrProtocol
		}
		for _, id := range m.RelationIDs {
			r, ok := d.relations[id]
			if !ok {
				return nil, ErrProtocol
			}
			if err = d.add(Change{Kind: "truncate", Relation: r}); err != nil {
				return nil, err
			}
		}
	case *pglogrepl.CommitMessage:
		if d.pending == nil || m.Flags != 0 || Position(m.CommitLSN) != d.pending.Begin || Position(m.TransactionEndLSN) <= Position(m.CommitLSN) || Position(m.TransactionEndLSN) <= d.last {
			return nil, ErrProtocol
		}
		d.pending.Commit = Position(m.CommitLSN)
		d.pending.End = Position(m.TransactionEndLSN)
		completed := d.pending
		d.pending = nil
		d.bytes = 0
		d.last = completed.End
		return completed, nil
	default:
		return nil, ErrProtocol
	}
	return nil, nil
}
