// Package logformats compares statement, physical and logical replication logs.
package logformats

import (
	"encoding/json"
	"errors"
	"regexp"

	"github.com/jackc/pglogrepl"
)

const MaxFrameBytes = 1 << 20
const MaxTransactionBytes = 4 << 20
const MaxChanges = 256
const MaxRelations = 16
const MaxColumns = 32

var (
	ErrProtocol   = errors.New("invalid or unsupported pgoutput v1 stream")
	ErrSchema     = errors.New("logical target schema contract differs; migrate or rebootstrap")
	ErrSource     = errors.New("logical source identity or retained position differs; rebootstrap required")
	ErrProjection = errors.New("logical row image cannot update the expected target row")
	ErrLimit      = errors.New("bounded logical replication limit exceeded")
)

type Position uint64

func (p Position) String() string { return pglogrepl.LSN(p).String() }
func ParsePosition(s string) (Position, error) {
	if !positionPattern.MatchString(s) {
		return 0, ErrProtocol
	}
	p, err := pglogrepl.ParseLSN(s)
	if err != nil {
		return 0, ErrProtocol
	}
	return Position(p), nil
}
func (p Position) MarshalJSON() ([]byte, error) { return json.Marshal(p.String()) }
func (p *Position) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) != nil {
		return ErrProtocol
	}
	n, err := ParsePosition(s)
	if err != nil {
		return err
	}
	*p = n
	return nil
}

type Identity struct {
	SystemID    string `json:"system_id"`
	Database    string `json:"database"`
	Slot        string `json:"slot"`
	Publication string `json:"publication"`
	Epoch       string `json:"epoch"`
}

var safeName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,47}$`)
var positionPattern = regexp.MustCompile(`^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$`)

type Column struct {
	Name     string `json:"name"`
	OID      uint32 `json:"type_oid"`
	Modifier int32  `json:"type_modifier"`
	Key      bool   `json:"key"`
}
type Relation struct {
	ID        uint32   `json:"relation_id"`
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Identity  byte     `json:"replica_identity"`
	Columns   []Column `json:"columns"`
}

func (r Relation) Qualified() string { return r.Namespace + "." + r.Name }

type Cell struct {
	Kind byte   `json:"kind"`
	Text string `json:"text,omitempty"`
}
type Change struct {
	Kind     string   `json:"kind"`
	Relation Relation `json:"relation"`
	Old      []Cell   `json:"old,omitempty"`
	New      []Cell   `json:"new,omitempty"`
}
type Transaction struct {
	XID     uint32   `json:"xid"`
	Begin   Position `json:"begin_final_lsn"`
	Commit  Position `json:"commit_lsn"`
	End     Position `json:"end_lsn"`
	Changes []Change `json:"changes"`
}
type Checkpoint struct {
	Identity Identity `json:"identity"`
	LSN      Position `json:"lsn"`
}
