package logformats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/childprocess"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/pgprocess"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type NativeOptions struct{ PGBin, PGShare, Binary, Workdir string }
type Observation struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}
type CrashEvidence struct {
	Phase                string   `json:"phase"`
	CheckpointBefore     Position `json:"checkpoint_before"`
	CheckpointAfterKill  Position `json:"checkpoint_after_kill"`
	ReplayedTransactions int      `json:"replayed_transactions_deduplicated"`
	Final                Position `json:"recovered_checkpoint"`
	SourceConfirmed      Position `json:"source_confirmed_after_kill"`
}
type Report struct {
	Postgres     string           `json:"postgres"`
	Identity     Identity         `json:"identity"`
	Bootstrap    Checkpoint       `json:"bootstrap"`
	Consumers    []ConsumerReport `json:"consumers"`
	Crashes      []CrashEvidence  `json:"crashes"`
	Observations []Observation    `json:"observations"`
	FinalDigest  string           `json:"matched_source_physical_logical_sha256"`
	Assumptions  []string         `json:"assumptions"`
}

func (r *Report) record(name string, value any) {
	r.Observations = append(r.Observations, Observation{name, value})
}

type nativeWorker struct {
	process *childprocess.Child
	job     WorkerJob
}

func startWorker(ctx context.Context, binary, dir, source, target string, identity Identity, marker, phase string, replay bool) (*nativeWorker, error) {
	job := WorkerJob{Identity: identity, Marker: marker, Phase: phase, Ready: filepath.Join(dir, marker+"-"+phase+"-ready.json"), Release: filepath.Join(dir, marker+"-never-release"), Output: filepath.Join(dir, marker+"-"+phase+"-result.json"), ReplayRetained: replay}
	path := filepath.Join(dir, marker+"-"+phase+"-job.json")
	if err := Save(path, job); err != nil {
		return nil, err
	}
	p, err := childprocess.Start(ctx, binary, dir, map[string]string{"ATLAS_LOG_SOURCE_URL": source, "ATLAS_LOG_TARGET_URL": target}, "worker", "--job", path)
	if err != nil {
		return nil, err
	}
	return &nativeWorker{p, job}, nil
}
func (w *nativeWorker) result(ctx context.Context) (WorkerResult, error) {
	waitErr := w.process.Wait(ctx)
	info, err := os.Stat(w.job.Output)
	if err != nil || info.Size() > MaxTransactionBytes {
		return WorkerResult{}, fmt.Errorf("logical consumer result unavailable: %v", waitErr)
	}
	body, err := os.ReadFile(w.job.Output)
	if err != nil {
		return WorkerResult{}, err
	}
	var result WorkerResult
	if json.Unmarshal(body, &result) != nil {
		return result, ErrProtocol
	}
	if waitErr != nil {
		return result, fmt.Errorf("logical consumer failed: %s", result.Error)
	}
	return result, nil
}
func (w *nativeWorker) ready(ctx context.Context) error {
	return pgprocess.Wait(ctx, "logical consumer boundary", func() (bool, error) {
		_, err := os.Stat(w.job.Ready)
		if err == nil {
			return true, nil
		}
		if w.process.Exited() {
			return false, errors.New("logical consumer exited before requested boundary")
		}
		if !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	})
}
func RunNative(ctx context.Context, o NativeOptions) (Report, error) {
	r := Report{Assumptions: []string{"three PostgreSQL 18 processes share one host and failure domain", "pgoutput protocol v1; non-streaming committed transactions with bounded text tuples", "snapshot import precedes reads and streaming; the logical slot retains overlapping commits", "target row effects, audit and checkpoint commit in one PostgreSQL transaction", "only durable target commit-end LSN is acknowledged; keepalive high-watermarks are not checkpoints", "physical replay includes WAL-backed schema/sequence state; logical DDL and sequence increments need separate policy", "logical slot failover, cross-major native tests, production CDC and scale remain open"}}
	if !filepath.IsAbs(o.Binary) {
		return r, ErrProtocol
	}
	cluster, err := pgprocess.New(ctx, pgprocess.Options{Bin: o.PGBin, Share: o.PGShare, Workdir: o.Workdir})
	if err != nil {
		return r, err
	}
	defer cluster.Close()
	fmt.Fprintln(os.Stderr, "log formats: primary, physical standby and logical projection")
	source, err := cluster.Init("source")
	if err != nil {
		return r, err
	}
	target, err := cluster.Init("target")
	if err != nil {
		return r, err
	}
	if err = cluster.Scalar(source, "SELECT version()", &r.Postgres); err != nil {
		return r, err
	}
	if err = cluster.Exec(source, Schema("log_lab")+SeedSQL+PublicationSQL); err != nil {
		return r, err
	}
	if err = cluster.Exec(target, Schema("log_lab")+Schema("statement_lab")); err != nil {
		return r, err
	}
	physical, err := cluster.Standby(source, "standby")
	if err != nil {
		return r, err
	}
	sourceContract, err := physicalContract(ctx, cluster, source)
	if err != nil {
		return r, err
	}
	replicaContract, err := physicalContract(ctx, cluster, physical)
	if err != nil {
		return r, err
	}
	targetContract, err := physicalContract(ctx, cluster, target)
	if err != nil {
		return r, err
	}
	if sourceContract.Major != 18 || targetContract.Major != 18 {
		return r, errors.New("native log-format profile requires PostgreSQL 18")
	}
	if err = CompatiblePhysical(sourceContract, replicaContract); err != nil {
		return r, err
	}
	if targetContract.SystemID == sourceContract.SystemID {
		return r, errors.New("logical target was not initialized as an independent cluster")
	}
	r.record("matched_physical_wal_contract", sourceContract)
	r.record("logical_projection_uses_independent_cluster_identity", true)
	var system string
	if err = cluster.Scalar(source, "SELECT system_identifier::text FROM pg_control_system()", &system); err != nil {
		return r, err
	}
	token := make([]byte, 16)
	if _, err = rand.Read(token); err != nil {
		return r, err
	}
	identity := Identity{system, "postgres", "atlas_log_rows", "atlas_log_publication", hex.EncodeToString(token)}
	r.Identity = identity
	if err = cluster.Exec(source, "INSERT INTO log_lab.stream_identity VALUES($1,$2)", identity.Slot, identity.Epoch); err != nil {
		return r, err
	}
	sourceURL, targetURL := cluster.URL(source, true), cluster.URL(target, true)
	sink, err := NewSink(ctx, targetURL, identity)
	if err != nil {
		return r, err
	}
	defer sink.Close()
	if err = sink.Init(ctx); err != nil {
		return r, err
	}
	baseline, err := ReadState(ctx, sourceURL)
	if err != nil {
		return r, err
	}
	r.Bootstrap, err = ExportBootstrap(ctx, sourceURL, sink, identity, func() error { return cluster.Exec(source, OverlapSQL) })
	if err != nil {
		return r, err
	}
	snapshot, err := ReadState(ctx, targetURL)
	if err != nil || snapshot.Digest != baseline.Digest {
		return r, errors.New("exported snapshot included later source commits or omitted seed rows")
	}
	r.record("exported_snapshot_excludes_overlapping_commit", true)
	if err = statementProbe(ctx, cluster, source, target, &r); err != nil {
		return r, err
	}
	if err = cluster.Exec(source, AbortedSQL+OrderedSQL); err != nil {
		return r, err
	}
	worker, err := startWorker(ctx, o.Binary, cluster.Dir, sourceURL, targetURL, identity, "ordered-transaction", "", false)
	if err != nil {
		return r, err
	}
	defer worker.process.Stop()
	result, err := worker.result(ctx)
	if err != nil || result.Outcome != "complete" {
		return r, fmt.Errorf("initial logical catch-up failed: %v %s", err, result.Outcome)
	}
	r.Consumers = append(r.Consumers, result.Report)
	if result.Report.ToastUnchanged == 0 {
		return r, errors.New("native fixture did not exercise unchanged TOAST")
	}
	if err = compareThree(ctx, cluster, source, target, physical, &r); err != nil {
		return r, err
	}
	r.record("snapshot_log_handoff_preserves_overlapping_commit", true)
	var operations []string
	rows, err := sink.Pool.Query(ctx, `SELECT operation||':'||split_part(relation,'.',2) FROM sink.changes WHERE end_lsn=(SELECT end_lsn FROM sink.changes WHERE relation='log_lab.scratch' AND operation='truncate') ORDER BY ordinal`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var op string
		if err = rows.Scan(&op); err != nil {
			rows.Close()
			return r, err
		}
		operations = append(operations, op)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	expected := []string{"insert:stock", "update:stock", "delete:stock", "update:stock", "update:stock", "truncate:scratch", "insert:scratch", "insert:barriers"}
	if !reflect.DeepEqual(operations, expected) {
		return r, fmt.Errorf("row-change ordering differs: %v", operations)
	}
	r.record("ordered_committed_operations", operations)
	var absent bool
	if err = cluster.Scalar(target, "SELECT NOT EXISTS(SELECT 1 FROM log_lab.receipts WHERE request_key='aborted')", &absent); err != nil || !absent {
		return r, errors.New("rolled-back source transaction reached logical projection")
	}
	r.record("rolled_back_rows_absent_and_sequence_gap_retained", true)
	var sequenceGap bool
	if err = cluster.Scalar(source, "SELECT last_value>(SELECT max(id) FROM log_lab.receipts) FROM log_lab.receipts_id_seq", &sequenceGap); err != nil || !sequenceGap {
		return r, errors.New("rolled-back sequence allocation did not leave an observable gap")
	}
	fmt.Fprintln(os.Stderr, "log formats: schema pause, migrate and resume")
	before, err := sink.Checkpoint(ctx)
	if err != nil {
		return r, err
	}
	targetBefore, err := ReadState(ctx, targetURL)
	if err != nil {
		return r, err
	}
	if err = cluster.Exec(source, "ALTER TABLE log_lab.stock REPLICA IDENTITY FULL; "+ExpandSQL); err != nil {
		return r, err
	}
	if err = cluster.Exec(source, ExpandedWriteSQL); err != nil {
		return r, err
	}
	paused, err := startWorker(ctx, o.Binary, cluster.Dir, sourceURL, targetURL, identity, "schema-expanded", "", false)
	if err != nil {
		return r, err
	}
	defer paused.process.Stop()
	pauseResult, err := paused.result(ctx)
	if err != nil || pauseResult.Outcome != "schema_mismatch" {
		return r, fmt.Errorf("logical schema mismatch was not explicit: %v %s", err, pauseResult.Outcome)
	}
	after, err := sink.Checkpoint(ctx)
	if err != nil || after.LSN != before.LSN {
		return r, errors.New("schema failure advanced checkpoint")
	}
	pausedState, err := ReadState(ctx, targetURL)
	if err != nil || pausedState.Digest != targetBefore.Digest {
		return r, errors.New("schema failure left partial row effects")
	}
	if err = cluster.Catchup(source, physical); err != nil {
		return r, err
	}
	var physicalColumn bool
	if err = cluster.Scalar(physical, "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='log_lab' AND table_name='stock' AND column_name='label')", &physicalColumn); err != nil || !physicalColumn {
		return r, errors.New("physical WAL did not reproduce source DDL")
	}
	if err = cluster.Exec(target, ExpandSQL); err != nil {
		return r, err
	}
	resumed, err := startWorker(ctx, o.Binary, cluster.Dir, sourceURL, targetURL, identity, "schema-expanded", "", false)
	if err != nil {
		return r, err
	}
	defer resumed.process.Stop()
	resumeResult, err := resumed.result(ctx)
	if err != nil || resumeResult.Outcome != "complete" {
		return r, fmt.Errorf("logical schema resume failed: %v %s", err, resumeResult.Outcome)
	}
	r.Consumers = append(r.Consumers, resumeResult.Report)
	r.record("logical_schema_pause_preserves_checkpoint_until_target_migration", true)
	r.record("physical_wal_replays_ddl_and_logical_relation_metadata_is_not_ddl", true)
	if err = compareThree(ctx, cluster, source, target, physical, &r); err != nil {
		return r, err
	}
	fmt.Fprintln(os.Stderr, "log formats: consumer process crash and retained replay")
	for i, phase := range []string{"before_commit", "after_commit"} {
		marker := fmt.Sprintf("crash_%d", i)
		if err = cluster.Exec(source, CrashSQL(marker)); err != nil {
			return r, err
		}
		before, err := sink.Checkpoint(ctx)
		if err != nil {
			return r, err
		}
		stateBefore, err := ReadState(ctx, targetURL)
		if err != nil {
			return r, err
		}
		var transactionsBefore, changesBefore int
		if err = cluster.Scalar(target, "SELECT count(*) FROM sink.transactions", &transactionsBefore); err != nil {
			return r, err
		}
		if err = cluster.Scalar(target, "SELECT count(*) FROM sink.changes", &changesBefore); err != nil {
			return r, err
		}
		crashing, err := startWorker(ctx, o.Binary, cluster.Dir, sourceURL, targetURL, identity, marker, phase, false)
		if err != nil {
			return r, err
		}
		defer crashing.process.Stop()
		if err = crashing.ready(ctx); err != nil {
			return r, err
		}
		crashing.process.Kill()
		if _, err = os.Stat(crashing.job.Output); !os.IsNotExist(err) {
			return r, errors.New("killed consumer unexpectedly returned acknowledgment")
		}
		if err = pgprocess.Wait(ctx, "consumer target connection rollback", func() (bool, error) {
			var n int
			e := cluster.Scalar(target, "SELECT count(*) FROM pg_stat_activity WHERE application_name='atlas-cdc-sink' AND state='idle in transaction'", &n)
			return n == 0, e
		}); err != nil {
			return r, err
		}
		after, err := sink.Checkpoint(ctx)
		if err != nil {
			return r, err
		}
		if (phase == "before_commit" && after.LSN != before.LSN) || (phase == "after_commit" && after.LSN <= before.LSN) {
			return r, errors.New("target checkpoint crossed the wrong crash boundary")
		}
		if phase == "before_commit" {
			stateAfter, e := ReadState(ctx, targetURL)
			if e != nil || stateAfter.Digest != stateBefore.Digest {
				return r, errors.New("uncommitted source batch left target row effects")
			}
		}
		if err = pgprocess.Wait(ctx, "logical slot consumer exit", func() (bool, error) {
			var active bool
			e := cluster.Scalar(source, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", &active, identity.Slot)
			return !active, e
		}); err != nil {
			return r, err
		}
		var sourceConfirmedText string
		if err = cluster.Scalar(source, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", &sourceConfirmedText, identity.Slot); err != nil {
			return r, err
		}
		sourceConfirmed, err := ParsePosition(sourceConfirmedText)
		if err != nil || sourceConfirmed > before.LSN {
			return r, errors.New("source acknowledgment crossed unacknowledged target boundary")
		}
		var transactionsAfter, changesAfter int
		if err = cluster.Scalar(target, "SELECT count(*) FROM sink.transactions", &transactionsAfter); err != nil {
			return r, err
		}
		if err = cluster.Scalar(target, "SELECT count(*) FROM sink.changes", &changesAfter); err != nil {
			return r, err
		}
		delta := 0
		if phase == "after_commit" {
			delta = 1
		}
		if transactionsAfter != transactionsBefore+delta || changesAfter != changesBefore+3*delta {
			return r, errors.New("target audit and checkpoint did not share transaction boundary")
		}
		replay, err := startWorker(ctx, o.Binary, cluster.Dir, sourceURL, targetURL, identity, marker, "", true)
		if err != nil {
			return r, err
		}
		defer replay.process.Stop()
		recovered, err := replay.result(ctx)
		if err != nil || recovered.Outcome != "complete" {
			return r, fmt.Errorf("retained replay failed: %v %s", err, recovered.Outcome)
		}
		if phase == "after_commit" && recovered.Report.Duplicates == 0 {
			return r, errors.New("retained source replay did not exercise durable deduplication")
		}
		r.Crashes = append(r.Crashes, CrashEvidence{phase, before.LSN, after.LSN, recovered.Report.Duplicates, recovered.Report.Last.LSN, sourceConfirmed})
		r.Consumers = append(r.Consumers, recovered.Report)
	}
	if err = compareThree(ctx, cluster, source, target, physical, &r); err != nil {
		return r, err
	}
	if err = finalProbes(ctx, cluster, source, target, physical, sink, o.Binary, identity, &r); err != nil {
		return r, err
	}
	return r, nil
}
func compareThree(ctx context.Context, c *pgprocess.Cluster, source, target, physical *pgprocess.Node, r *Report) error {
	if err := c.Catchup(source, physical); err != nil {
		return err
	}
	a, err := ReadState(ctx, c.URL(source, true))
	if err != nil {
		return err
	}
	b, err := ReadState(ctx, c.URL(target, true))
	if err != nil {
		return err
	}
	p, err := ReadState(ctx, c.URL(physical, true))
	if err != nil {
		return err
	}
	if a.Digest != b.Digest || a.Digest != p.Digest || a.StockQuantity < 0 || a.PayloadHash != b.PayloadHash {
		return fmt.Errorf("source/physical/logical state differs: %s %s %s", a.Digest, b.Digest, p.Digest)
	}
	r.FinalDigest = a.Digest
	return nil
}
func statementProbe(ctx context.Context, c *pgprocess.Cluster, source, target *pgprocess.Node, r *Report) error {
	for _, item := range []struct {
		node                      *pgprocess.Node
		namespace, timezone, seed string
		sequence                  int64
	}{{source, "log_lab", "Etc/GMT+12", "0.125", 1000}, {target, "statement_lab", "Pacific/Kiritimati", "-0.625", 2000}} {
		conn, err := c.Connect(item.node)
		if err != nil {
			return err
		}
		sql := "SET search_path=" + pgx.Identifier{item.namespace}.Sanitize() + "; SET timezone=" + literal(item.timezone) + "; SELECT setseed(" + item.seed + "); SELECT setval(" + literal(item.namespace+".receipts_id_seq") + "," + fmt.Sprint(item.sequence) + ",true); " + ClockStatement
		_, err = conn.Exec(ctx, sql)
		conn.Close(context.Background())
		if err != nil {
			return err
		}
	}
	a, err := FieldsOfReceipt(ctx, c.URL(source, true), "log_lab")
	if err != nil {
		return err
	}
	b, err := FieldsOfReceipt(ctx, c.URL(target, true), "statement_lab")
	if err != nil {
		return err
	}
	differences := []string{}
	for _, field := range []string{"id", "recorded_at", "day_label", "sample"} {
		if a[field] != b[field] {
			differences = append(differences, field)
		}
	}
	if len(differences) != 4 {
		return fmt.Errorf("nondeterministic SQL fixture did not diverge: %v", differences)
	}
	r.record("same_statement_divergent_fields", differences)
	return nil
}
func finalProbes(ctx context.Context, c *pgprocess.Cluster, source, target, physical *pgprocess.Node, sink *Sink, binary string, identity Identity, r *Report) error {
	var transactions, changes int
	if err := c.Scalar(target, "SELECT count(*) FROM sink.transactions", &transactions); err != nil {
		return err
	}
	if err := c.Scalar(target, "SELECT count(*) FROM sink.changes", &changes); err != nil {
		return err
	}
	state, err := ReadState(ctx, c.URL(source, true))
	if err != nil {
		return err
	}
	if transactions != 6 || changes != 21 || state.Receipts != 5 || state.StockQuantity != 24 {
		return fmt.Errorf("final logical invariant counts differ: %d transactions %d changes %d receipts %d quantity", transactions, changes, state.Receipts, state.StockQuantity)
	}
	r.record("applied_source_transactions", transactions)
	r.record("applied_row_changes", changes)
	r.record("final_source_receipts", state.Receipts)
	r.record("final_book_quantity", state.StockQuantity)
	if err = c.Exec(source, `CREATE TABLE log_lab.unkeyed(value text); CREATE PUBLICATION invalid_identity FOR TABLE log_lab.unkeyed; INSERT INTO log_lab.unkeyed VALUES('before')`); err != nil {
		return err
	}
	err = c.Exec(source, "UPDATE log_lab.unkeyed SET value='after'")
	var identityError *pgconn.PgError
	if !errors.As(err, &identityError) || identityError.Code != "55000" {
		return fmt.Errorf("missing replica identity did not fail explicitly: %v", err)
	}
	if err = c.Exec(source, "DROP PUBLICATION invalid_identity; DROP TABLE log_lab.unkeyed"); err != nil {
		return err
	}
	r.record("published_update_without_replica_identity_is_rejected", true)
	if err = c.Catchup(source, physical); err != nil {
		return err
	}
	checkpoint, err := sink.Checkpoint(ctx)
	if err != nil {
		return err
	}
	if err = pgprocess.Wait(ctx, "source acknowledges durable target position", func() (bool, error) {
		var value string
		e := c.Scalar(source, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", &value, identity.Slot)
		if e != nil {
			return false, e
		}
		p, e := ParsePosition(value)
		return p >= checkpoint.LSN, e
	}); err != nil {
		return err
	}
	wrong := identity
	wrong.Epoch = "different-incarnation"
	probe, err := startWorker(ctx, binary, c.Dir, c.URL(source, true), c.URL(target, true), wrong, "wrong-source", "", false)
	if err != nil {
		return err
	}
	defer probe.process.Stop()
	result, err := probe.result(ctx)
	if err != nil || result.Outcome != "source_mismatch" {
		return errors.New("checkpoint source identity was not fenced")
	}
	r.record("checkpoint_identity_mismatch_rejected", true)
	if err = pgprocess.Wait(ctx, "final logical slot consumer exit", func() (bool, error) {
		var active bool
		e := c.Scalar(source, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", &active, identity.Slot)
		return !active, e
	}); err != nil {
		return err
	}
	if err = c.Exec(source, "SELECT pg_drop_replication_slot($1)", identity.Slot); err != nil {
		return err
	}
	if _, err = ValidateSource(ctx, c.URL(source, true), identity, checkpoint); !errors.Is(err, ErrSource) {
		return errors.New("missing logical slot silently resumed")
	}
	r.record("lost_logical_slot_requires_rebootstrap", true)
	c.Stop(source)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", source.Port), 100*time.Millisecond)
	if err == nil {
		conn.Close()
		return errors.New("source still writable before branch promotion probes")
	}
	// Independent offline branch probes, after the original source is fenced.
	_, err = sink.Pool.Exec(ctx, "INSERT INTO log_lab.receipts(request_key,sku,quantity) VALUES('logical-promotion-probe','BOOK',1)")
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "23505" {
		return fmt.Errorf("logical sequence state did not expose promotion collision: %v", err)
	}
	if _, err = sink.Pool.Exec(ctx, "SELECT setval('log_lab.receipts_id_seq',(SELECT max(id) FROM log_lab.receipts),true)"); err != nil {
		return err
	}
	var logicalID int64
	if err = sink.Pool.QueryRow(ctx, "INSERT INTO log_lab.receipts(request_key,sku,quantity) VALUES('logical-promotion-probe','BOOK',1) RETURNING id").Scan(&logicalID); err != nil {
		return err
	}
	if err = c.Command("pg_ctl", "-D", physical.Dir, "-w", "promote"); err != nil {
		return err
	}
	var physicalID int64
	if err = c.Scalar(physical, "INSERT INTO log_lab.receipts(request_key,sku,quantity) VALUES('physical-promotion-probe','BOOK',1) RETURNING id", &physicalID); err != nil {
		return err
	}
	r.record("logical_sequence_not_replicated_collision_then_explicit_repair", logicalID)
	r.record("physical_sequence_preserves_noncolliding_promotion_id", physicalID)
	r.record("physical_sequence_values_may_jump_due_to_wal_preallocation", true)
	return nil
}
