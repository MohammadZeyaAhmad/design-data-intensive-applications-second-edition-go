// Package evolution runs actual historical/current Atlas processes against an
// owned PostgreSQL database through expand, backfill, rollback and contraction.
package evolution

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type rollout struct {
	Phase           string `json:"phase"`
	Pending         int64  `json:"pending"`
	Batches         int64  `json:"batches"`
	Rows            int64  `json:"rows_backfilled"`
	LegacyReaders   int    `json:"unretired_legacy_readers"`
	LegacySessions  int    `json:"legacy_database_sessions"`
	RollbackRetired bool   `json:"rollback_retired"`
}
type batch struct {
	ID        string `json:"id"`
	Rows      int    `json:"rows"`
	Pending   bool   `json:"pending"`
	Replay    bool   `json:"replay"`
	Throttled bool   `json:"throttled"`
	WallNanos int64  `json:"process_wall_nanos"`
}
type Report struct {
	Scenario               string          `json:"scenario"`
	Build                  json.RawMessage `json:"binary_builds"`
	SeededBrands           int             `json:"seeded_brands"`
	Phases                 []rollout       `json:"phases"`
	Batches                []batch         `json:"batches"`
	KilledBeforeCommit     bool            `json:"killed_before_commit"`
	LostReplyReplayed      bool            `json:"lost_reply_replayed"`
	ThrottleObserved       bool            `json:"durable_throttle_observed"`
	LockedRowRetried       bool            `json:"locked_row_retried"`
	ConcurrentWrites       int             `json:"foreground_writes"`
	Foreground             []writeSample   `json:"foreground_samples"`
	MaxForegroundNanos     int64           `json:"foreground_max_wall_nanos"`
	RollbackOldProcess     bool            `json:"historical_binary_rollback"`
	EarlyContractRejected  bool            `json:"early_contraction_rejected"`
	RetirementGates        bool            `json:"reader_and_window_retirement_verified"`
	LegacyColumnRemoved    bool            `json:"legacy_column_removed"`
	CurrentAfterContract   bool            `json:"current_binary_after_contract"`
	SourceHistoryUnchanged bool            `json:"historical_source_digests_unchanged"`
	ArchivePath            string          `json:"archive_path"`
	ArchiveSHA256          string          `json:"archive_sha256"`
	ArchiveRecords         int             `json:"archived_records"`
	ArchiveSchemas         []int           `json:"archived_writer_versions"`
	ArchiveReplayed        bool            `json:"offline_archive_replayed_after_contract"`
	Boundaries             []string        `json:"boundaries"`
}
type operator struct{ binary, dir, dsn string }

func (o operator) run(ctx context.Context, args ...string) ([]byte, error) {
	return runCommand(ctx, o.binary, o.dir, map[string]string{"ATLAS_SCHEMA_DATABASE_URL": o.dsn}, args...)
}
func (o operator) status(ctx context.Context) (rollout, error) {
	raw, e := o.run(ctx, "status")
	var result rollout
	if e == nil {
		e = json.Unmarshal(raw, &result)
	}
	return result, e
}
func (o operator) batch(ctx context.Context, id string, interval string) (batch, error) {
	start := time.Now()
	raw, e := o.run(ctx, "batch", "-id", id, "-limit", "16", "-interval", interval)
	var result batch
	if e == nil {
		e = json.Unmarshal(raw, &result)
	}
	result.WallNanos = time.Since(start).Nanoseconds()
	return result, e
}
func token() string {
	var b [32]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func taggedDSN(raw, name string) string {
	u, _ := url.Parse(raw)
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	return u.String()
}
func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func Run(ctx context.Context, dsn, binaries, parent string, rows int) (report Report, runErr error) {
	u, e := url.Parse(dsn)
	if e != nil || dsn == "" || u.Path != "/atlas_test" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "postgres") {
		return report, errors.New("bind ATLAS_TEST_DATABASE_URL to a local atlas_test database")
	}
	if rows < 32 || rows > 512 {
		return report, errors.New("evolution rows must be 32..512")
	}
	binaries, e = filepath.Abs(binaries)
	if e != nil {
		return report, e
	}
	build, e := os.ReadFile(filepath.Join(binaries, "manifest.json"))
	if e != nil {
		return report, e
	}
	var manifest struct {
		Legacy   string            `json:"legacy_revision"`
		Binaries map[string]string `json:"binaries"`
	}
	if json.Unmarshal(build, &manifest) != nil || manifest.Legacy != "393f703d7e097f96f09847b8196ead2c4429f260" {
		return report, errors.New("unexpected historical binary manifest")
	}
	for _, name := range []string{"legacy-atlas", "current-atlas", "schema"} {
		raw, e := os.ReadFile(filepath.Join(binaries, name))
		if e != nil {
			return report, e
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != manifest.Binaries[name] {
			return report, errors.New("binary digest does not match manifest")
		}
	}
	if manifest.Binaries["legacy-atlas"] == manifest.Binaries["current-atlas"] {
		return report, errors.New("drill requires distinct actual binaries")
	}
	if parent == "" {
		parent = os.TempDir()
	}
	if e = os.MkdirAll(parent, 0700); e != nil {
		return report, e
	}
	dir, e := os.MkdirTemp(parent, "evolution-run-")
	if e != nil {
		return report, e
	}
	dir, e = filepath.Abs(dir)
	if e != nil {
		return report, e
	}
	retained := false
	defer func() {
		if !retained {
			os.RemoveAll(dir)
		}
	}()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		return report, errors.New("could not initialize evolution administrator")
	}
	defer admin.Close()
	database := "atlas_evolution_" + token()[:24]
	if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); e != nil {
		return report, errors.New("could not create owned evolution database")
	}
	var old, current *server
	var owned *pgxpool.Pool
	defer func() {
		old.stop()
		current.stop()
		if owned != nil {
			owned.Close()
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, e := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)"); e != nil && runErr == nil {
			runErr = errors.New("owned evolution database cleanup failed")
		}
	}()
	u.Path = "/" + database
	privateDSN := u.String()
	owned, e = pgxpool.New(ctx, taggedDSN(privateDSN, "atlas-evolution/control"))
	if e != nil {
		return report, e
	}
	tenantToken, otherToken, providerToken, cursorKey := token(), token(), token(), token()
	baseEnv := func(application string) map[string]string {
		return map[string]string{"ATLAS_DATABASE_URL": taggedDSN(privateDSN, application), "ATLAS_TENANT_KEYS": "acme=" + tenantToken + ",globex=" + otherToken, "ATLAS_PROVIDER_TOKEN": providerToken, "ATLAS_PROVIDER_URL": "http://127.0.0.1:18999", "ATLAS_GRAPHQL_CURSOR_KEY": cursorKey, "ATLAS_CHECKOUT_ENGINE": "postgres", "ATLAS_MESSAGE_TRANSPORT": "postgres", "ATLAS_SEARCH_VECTOR_ENABLED": "false"}
	}
	legacyBinary, currentBinary := filepath.Join(binaries, "legacy-atlas"), filepath.Join(binaries, "current-atlas")
	op := operator{filepath.Join(binaries, "schema"), dir, taggedDSN(privateDSN, "atlas-evolution/operator")}
	if _, e = runCommand(ctx, legacyBinary, dir, baseEnv("atlas-evolution/migrate-old"), "migrate"); e != nil {
		return report, e
	}
	old, e = startServer(ctx, legacyBinary, dir, baseEnv("atlas-legacy/legacy-main"), tenantToken)
	if e != nil {
		return report, e
	}
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("maker-%03d", i)
		if _, _, e = old.put(ctx, id, "seed-"+id, "Original "+id, 0); e != nil {
			return report, e
		}
	}
	if _, _, e = old.put(ctx, "live-maker", "seed-live", "Original live", 0); e != nil {
		return report, e
	}
	old.token = otherToken
	if _, _, e = old.put(ctx, "maker-000", "other-tenant", "Other tenant", 0); e != nil {
		return report, e
	}
	old.token = tenantToken
	if _, e = runCommand(ctx, currentBinary, dir, baseEnv("atlas-evolution/migrate-new"), "migrate"); e != nil {
		return report, e
	}
	current, e = startServer(ctx, currentBinary, dir, baseEnv("atlas-next/canonical-main"), tenantToken)
	if e != nil {
		return report, e
	}
	report = Report{Scenario: "rolling-schema-evolution", Build: build, SeededBrands: rows + 2, Boundaries: []string{
		"Actual Atlas 393f703 and current executables run concurrently against a fresh owned database; HTTP brand contracts remain stable while SQL/history formats evolve.",
		"Backfill uses 16-row batches, a durable pending predicate and receipt, 300ms SQL/idle-transaction limits and a 1s transaction context. HTTP probes use a 2s request deadline; raw observed latency is retained, without a production SLA claim.",
		"Concurrent index preparation and two-transaction constraint validation are real PostgreSQL operations. Contraction is explicit and gated by validated data, declared legacy-reader retirement, observed tagged sessions and rollback-window retirement.",
		"Reader inventory and session labels are trusted deployment-operator assertions, not a substitute for production version-specific database roles or fencing arbitrary clients.",
		"The isolated fixture uses a short real rollback window. Normal operator CLI defaults to seven days and cannot shorten an already opened window.",
		"An owned database trigger pauses the receipt insert; SIGKILL exercises a real uncommitted worker transaction. A discarded committed reply is resolved by a separate process replaying its receipt.",
		"Archived JSONB source images, metadata, schema definitions and hashes remain immutable; offline replay uses explicit old/new upcasters, independently of current table columns. This is a bounded local archive, not a full database backup or WORM service.",
	}}
	state, e := op.status(ctx)
	if e != nil {
		return report, e
	}
	if state.Pending != int64(rows+2) {
		return report, errors.New("legacy rows were eagerly rewritten during expand")
	}
	report.Phases = append(report.Phases, state)
	legacyFirst, _, e := old.call(ctx, "GET", "maker-000", "", nil)
	if e != nil {
		return report, e
	}
	canonicalFirst, _, e := current.call(ctx, "GET", "maker-000", "", nil)
	if e != nil || legacyFirst != canonicalFirst {
		return report, errors.New("new reader did not fall back to legacy state")
	}
	if _, e = op.run(ctx, "lease", "-id", "legacy-main", "-capability", "1", "-ttl", "400ms"); e != nil {
		return report, e
	}
	if _, e = op.run(ctx, "prepare"); e != nil {
		return report, e
	}
	newFact, _, e := current.put(ctx, "live-maker", "current-live", "Current live", 1)
	if e != nil {
		return report, e
	}
	observed, _, e := old.call(ctx, "GET", "live-maker", "", nil)
	if e != nil || newFact != observed {
		return report, errors.New("old binary could not read new write")
	}
	oldFact, _, e := old.put(ctx, "live-maker", "legacy-live", "Legacy live", newFact.Version)
	if e != nil {
		return report, e
	}
	observed, _, e = current.call(ctx, "GET", "live-maker", "", nil)
	if e != nil || oldFact != observed {
		return report, errors.New("new binary could not read old write")
	}
	if _, e = op.run(ctx, "contract", "-reason", "intentionally too early"); e == nil {
		return report, errors.New("early contraction was allowed")
	}
	report.EarlyContractRejected = true
	var sourceBefore string
	if e = owned.QueryRow(ctx, `SELECT md5(string_agg(body::text||metadata::text,'|' ORDER BY id,version)) FROM catalog.content_history WHERE tenant_id='acme' AND version=1`).Scan(&sourceBefore); e != nil {
		return report, e
	}
	hold, e := owned.Begin(ctx)
	if e != nil {
		return report, e
	}
	defer hold.Rollback(context.Background())
	if _, e = hold.Exec(ctx, `SELECT 1 FROM catalog.brands WHERE tenant_id='acme' AND id='maker-000' FOR UPDATE`); e != nil {
		return report, e
	}
	if e = crashBeforeCommit(ctx, op, owned); e != nil {
		return report, e
	}
	report.KilledBeforeCommit = true
	// Commit a real batch while discarding its reply, then resolve it elsewhere.
	if _, e = op.run(ctx, "run", "-id", "committed-reply-lost", "-max-batches", "1", "-limit", "16", "-interval", "500ms"); e != nil {
		return report, e
	}
	replayed, e := op.batch(ctx, "committed-reply-lost-000000", "500ms")
	if e != nil || !replayed.Replay || replayed.Rows != 16 {
		return report, errors.New("committed batch receipt did not replay")
	}
	report.LostReplyReplayed = true
	report.Batches = append(report.Batches, replayed)
	throttled, e := op.batch(ctx, "too-soon", "500ms")
	if e != nil || !throttled.Throttled {
		return report, errors.New("cross-process throttle did not persist")
	}
	report.ThrottleObserved = true
	// Real old/new foreground writes overlap the separate backfill processes.
	foreground, _, e := current.call(ctx, "GET", "maker-001", "", nil)
	if e != nil {
		return report, e
	}
	stopWriters := foregroundWriters(ctx, old, current, foreground)
	defer stopWriters()
	for i := 0; i < 80; i++ {
		result, e := op.batch(ctx, fmt.Sprintf("resume-%03d", i), "20ms")
		if e != nil {
			return report, e
		}
		if result.Throttled {
			if e = pause(ctx, 30*time.Millisecond); e != nil {
				return report, e
			}
			continue
		}
		report.Batches = append(report.Batches, result)
		if result.Rows == 0 && result.Pending {
			break
		}
		if !result.Pending {
			return report, errors.New("locked row disappeared from pending predicate")
		}
	}
	writes := stopWriters()
	if writes.Err != nil {
		return report, writes.Err
	}
	report.Foreground = writes.Samples
	report.ConcurrentWrites = len(writes.Samples)
	if report.ConcurrentWrites < 2 {
		return report, errors.New("foreground writes did not overlap the migration")
	}
	for _, sample := range writes.Samples {
		if sample.WallNanos > report.MaxForegroundNanos {
			report.MaxForegroundNanos = sample.WallNanos
		}
	}
	state, e = op.status(ctx)
	if e != nil || state.Pending != 1 {
		return report, errors.New("backfill did not preserve exactly the locked pending row")
	}
	report.Phases = append(report.Phases, state)
	if _, e = op.run(ctx, "validate", "-reason", "locked row remains"); e == nil {
		return report, errors.New("validation accepted incomplete backfill")
	}
	current.stop()
	old.stop()
	old, e = startServer(ctx, legacyBinary, dir, baseEnv("atlas-legacy/legacy-main"), tenantToken)
	if e != nil {
		return report, e
	}
	rollbackFact, _, e := old.put(ctx, "live-maker", "rollback-write", "Rollback writer", oldFact.Version)
	if e != nil || rollbackFact.Version != oldFact.Version+1 {
		return report, errors.New("actual historical binary rollback failed")
	}
	report.RollbackOldProcess = true
	if e = hold.Rollback(ctx); e != nil {
		return report, e
	}
	if e = pause(ctx, 30*time.Millisecond); e != nil {
		return report, e
	}
	last, e := op.batch(ctx, "previously-locked", "20ms")
	if e != nil || last.Rows != 1 || last.Pending {
		return report, errors.New("resumed backfill skipped the formerly locked row")
	}
	report.LockedRowRetried = true
	report.Batches = append(report.Batches, last)
	state, e = op.status(ctx)
	if e != nil || state.Phase != "backfilled" || state.Pending != 0 {
		return report, errors.New("completed backfill state was not durable")
	}
	report.Phases = append(report.Phases, state)
	if _, e = op.run(ctx, "validate", "-reason", "all rows reconciled"); e != nil {
		return report, e
	}
	state, e = op.status(ctx)
	if e != nil || state.Phase != "validated" {
		return report, errors.New("validation did not publish")
	}
	report.Phases = append(report.Phases, state)
	if _, e = op.run(ctx, "window", "-window", "3s", "-reason", "short isolated demonstration"); e != nil {
		return report, e
	}
	if _, e = op.run(ctx, "retire-reader", "-id", "legacy-main", "-reason", "process still active"); e == nil {
		return report, errors.New("active legacy database sessions were ignored")
	}
	old.stop()
	if _, e = op.run(ctx, "retire-reader", "-id", "legacy-main", "-reason", "historical process drained"); e != nil {
		return report, e
	}
	if _, e = op.run(ctx, "retire-rollback", "-reason", "deadline not reached"); e == nil {
		return report, errors.New("rollback window expired before fixture expected")
	}
	current, e = startServer(ctx, currentBinary, dir, baseEnv("atlas-next/canonical-main"), tenantToken)
	if e != nil {
		return report, e
	}
	archive := filepath.Join(dir, "brand-history.json")
	rawExport, e := op.run(ctx, "export", "-tenant", "acme", "-brand", "live-maker", "-file", archive)
	if e != nil {
		return report, e
	}
	var exported struct {
		Records int    `json:"records"`
		SHA     string `json:"archive_sha256"`
	}
	if json.Unmarshal(rawExport, &exported) != nil {
		return report, errors.New("invalid archive export response")
	}
	report.ArchivePath = archive
	report.ArchiveSHA256 = exported.SHA
	report.ArchiveRecords = exported.Records
	var archived struct {
		Records []struct {
			Schema int `json:"schema_version"`
		} `json:"records"`
	}
	archiveBytes, e := os.ReadFile(archive)
	if e != nil || json.Unmarshal(archiveBytes, &archived) != nil {
		return report, errors.New("archive was not retained")
	}
	seen := map[int]bool{}
	for _, record := range archived.Records {
		seen[record.Schema] = true
	}
	if !seen[1] || !seen[2] {
		return report, errors.New("archive did not retain both historical writer versions")
	}
	report.ArchiveSchemas = []int{1, 2}
	deadline := time.Now().Add(6 * time.Second)
	for {
		if _, e = op.run(ctx, "retire-rollback", "-reason", "reader drained and elapsed window"); e == nil {
			break
		}
		if time.Now().After(deadline) {
			return report, e
		}
		if e = pause(ctx, 50*time.Millisecond); e != nil {
			return report, e
		}
	}
	if _, e = op.run(ctx, "contract", "-reason", "explicit retirement and validation complete"); e != nil {
		return report, e
	}
	state, e = op.status(ctx)
	if e != nil || state.Phase != "contracted" || !state.RollbackRetired || state.LegacyReaders != 0 || state.LegacySessions != 0 {
		return report, errors.New("contraction preconditions not retained")
	}
	report.Phases = append(report.Phases, state)
	report.RetirementGates = true
	var exists bool
	if e = owned.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='catalog' AND table_name='brands' AND column_name='name')`).Scan(&exists); e != nil || exists {
		return report, errors.New("legacy column was not removed")
	}
	report.LegacyColumnRemoved = true
	if _, e = op.run(ctx, "lease", "-id", "legacy-main", "-capability", "1", "-ttl", "1s"); e == nil {
		return report, errors.New("retired legacy capability was revived")
	}
	final, _, e := current.put(ctx, "live-maker", "contracted-write", "After contraction", rollbackFact.Version)
	if e != nil || final.Version != rollbackFact.Version+1 {
		return report, errors.New("current binary failed after contraction")
	}
	report.CurrentAfterContract = true
	replayedArchive, e := runCommand(ctx, op.binary, dir, nil, "replay", "-file", archive)
	if e != nil {
		return report, e
	}
	var replayedFacts []struct {
		ID      string `json:"id"`
		Name    string `json:"display_name"`
		Version int64  `json:"version"`
	}
	if json.Unmarshal(replayedArchive, &replayedFacts) != nil || len(replayedFacts) != report.ArchiveRecords || replayedFacts[len(replayedFacts)-1].Name != "Rollback writer" {
		return report, errors.New("offline archived upcast failed")
	}
	afterArchive, _ := os.ReadFile(archive)
	sum := sha256.Sum256(afterArchive)
	if hex.EncodeToString(sum[:]) != report.ArchiveSHA256 {
		return report, errors.New("replay rewrote archived source")
	}
	report.ArchiveReplayed = true
	var sourceAfter string
	if e = owned.QueryRow(ctx, `SELECT md5(string_agg(body::text||metadata::text,'|' ORDER BY id,version)) FROM catalog.content_history WHERE tenant_id='acme' AND version=1`).Scan(&sourceAfter); e != nil || sourceBefore != sourceAfter {
		return report, errors.New("migration/upcast rewrote historical source")
	}
	report.SourceHistoryUnchanged = true
	var bad int
	if e = owned.QueryRow(ctx, `SELECT count(*) FROM catalog.brands WHERE version<>1 AND id NOT IN ('live-maker','maker-001')`).Scan(&bad); e != nil || bad != 0 {
		return report, errors.New("backfill advanced business versions")
	}
	retained = true
	return report, nil
}

func crashBeforeCommit(ctx context.Context, op operator, pool *pgxpool.Pool) error {
	var pendingBefore int64
	if e := pool.QueryRow(ctx, `SELECT count(*) FROM catalog.brands WHERE display_name IS NULL`).Scan(&pendingBefore); e != nil {
		return e
	}
	if _, e := pool.Exec(ctx, `CREATE FUNCTION catalog.pause_evolution_receipt() RETURNS trigger LANGUAGE plpgsql AS $pause$ BEGIN IF NEW.batch_id='crash-before-commit' THEN PERFORM pg_sleep(10); END IF; RETURN NEW; END $pause$; CREATE TRIGGER pause_evolution_receipt BEFORE INSERT ON catalog.brand_backfill_receipts FOR EACH ROW EXECUTE FUNCTION catalog.pause_evolution_receipt()`); e != nil {
		return e
	}
	defer pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS pause_evolution_receipt ON catalog.brand_backfill_receipts; DROP FUNCTION IF EXISTS catalog.pause_evolution_receipt()`)
	cmd := exec.Command(op.binary, "batch", "-id", "crash-before-commit", "-limit", "16", "-interval", "20ms")
	cmd.Dir = op.dir
	cmd.Env = childEnvironment(map[string]string{"ATLAS_SCHEMA_DATABASE_URL": taggedDSN(op.dsn, "atlas-evolution/crash")})
	if e := cmd.Start(); e != nil {
		return e
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer cmd.Process.Kill()
	observed := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var sleeping bool
		if e := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='atlas-evolution/crash' AND wait_event='PgSleep')`).Scan(&sleeping); e != nil {
			return e
		}
		if sleeping {
			observed = true
			break
		}
		select {
		case <-done:
			return errors.New("fault worker exited before pause was observed")
		default:
		}
		if e := pause(ctx, 5*time.Millisecond); e != nil {
			return e
		}
	}
	if !observed {
		return errors.New("fault boundary was not observed")
	}
	if e := cmd.Process.Kill(); e != nil {
		return e
	}
	<-done
	active := true
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if e := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='atlas-evolution/crash')`).Scan(&active); e != nil {
			return e
		}
		if !active {
			break
		}
		if e := pause(ctx, 10*time.Millisecond); e != nil {
			return e
		}
	}
	if active {
		return errors.New("killed transaction did not release its database session within the bound")
	}
	var receipts int
	if e := pool.QueryRow(ctx, `SELECT count(*) FROM catalog.brand_backfill_receipts WHERE batch_id='crash-before-commit'`).Scan(&receipts); e != nil {
		return e
	}
	if receipts != 0 {
		return errors.New("uncommitted killed batch published a receipt")
	}
	var pendingAfter int64
	if e := pool.QueryRow(ctx, `SELECT count(*) FROM catalog.brands WHERE display_name IS NULL`).Scan(&pendingAfter); e != nil {
		return e
	}
	if pendingBefore != pendingAfter {
		return errors.New("killed batch published data without its receipt")
	}
	return nil
}
