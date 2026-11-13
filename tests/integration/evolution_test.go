//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	db "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/features/describe"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func evolutionStore(t *testing.T, legacyRows int) *db.Store {
	t.Helper()
	ctx := context.Background()
	id, e := identity.NewID()
	if e != nil {
		t.Fatal(e)
	}
	name := "atlas_evolution_test_" + strings.ReplaceAll(id, "-", "")
	if _, e = pool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	cfg := pool.Config().Copy()
	cfg.ConnConfig.Database = name
	owned, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		owned.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := pool.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); e != nil {
			t.Error(e)
		}
	})
	if e = migrations.Apply(ctx, owned); e != nil {
		t.Fatal(e)
	}
	// Model pre-expansion tuples in this isolated fixture. The process-level
	// drill additionally creates them with the actual historical Atlas binary.
	if legacyRows > 0 {
		tx, e := owned.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback(ctx)
		if _, e = tx.Exec(ctx, `ALTER TABLE catalog.brands DISABLE TRIGGER bridge_brand_names`); e != nil {
			t.Fatal(e)
		}
		for i := 0; i < legacyRows; i++ {
			if _, e = tx.Exec(ctx, `INSERT INTO catalog.brands(tenant_id,id,name,version) VALUES('acme',$1,$2,1)`, fmt.Sprintf("maker-%03d", i), fmt.Sprintf("Maker %03d", i)); e != nil {
				t.Fatal(e)
			}
		}
		if _, e = tx.Exec(ctx, `ALTER TABLE catalog.brands ENABLE TRIGGER bridge_brand_names`); e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(e)
		}
	}
	return &db.Store{Pool: owned}
}
func TestCatalogEvolutionBridgeAndVersionedHistory(t *testing.T) {
	s := evolutionStore(t, 1)
	ctx := context.Background()
	handler := describe.Handler{Store: s}
	if got, e := s.Brand(ctx, "acme", "maker-000"); e != nil || got.Name != "Maker 000" {
		t.Fatal("new reader did not fall back", got, e)
	}
	if _, e := s.Pool.Exec(ctx, `UPDATE catalog.brands SET name='Legacy update',version=version+1 WHERE tenant_id='acme' AND id='maker-000'`); e != nil {
		t.Fatal(e)
	}
	if got, e := s.Brand(ctx, "acme", "maker-000"); e != nil || got.Name != "Legacy update" {
		t.Fatal(got, e)
	}
	brand, _, e := handler.Brand(ctx, "acme", "current-update", domain.Brand{ID: "maker-000", Name: "Current update", Version: 2})
	if e != nil {
		t.Fatal(e)
	}
	var oldName, newName string
	if e = s.Pool.QueryRow(ctx, `SELECT name,display_name FROM catalog.brands WHERE tenant_id='acme' AND id='maker-000'`).Scan(&oldName, &newName); e != nil || oldName != brand.Name || newName != brand.Name {
		t.Fatal("new writer lost old representation", e)
	}
	if _, e = s.Pool.Exec(ctx, `UPDATE catalog.brands SET name='conflict-one',display_name='conflict-two' WHERE tenant_id='acme' AND id='maker-000'`); e == nil {
		t.Fatal("inconsistent pair accepted")
	}
	rows, e := s.BrandHistory(ctx, "acme", "maker-000")
	if e != nil || len(rows) != 1 || rows[0].SchemaVersion != 2 {
		t.Fatal(rows, e)
	}
	before := rows[0].SourceSHA256
	encoded, e := api.EncodeBrandArchive(rows)
	if e != nil {
		t.Fatal(e)
	}
	out, e := api.ReplayBrandArchive(encoded)
	if e != nil || len(out) != 1 || out[0].DisplayName != "Current update" {
		t.Fatal(out, e)
	}
	rows, e = s.BrandHistory(ctx, "acme", "maker-000")
	if e != nil || rows[0].SourceSHA256 != before {
		t.Fatal("upcast changed stored bytes", e)
	}
	if rows, e = s.BrandHistory(ctx, "globex", "maker-000"); e != nil || len(rows) != 0 {
		t.Fatal("history crossed tenant", e)
	}
}
func TestCatalogEvolutionBackfillSkipsLocksAndResumes(t *testing.T) {
	s := evolutionStore(t, 17)
	ctx := context.Background()
	if e := s.PrepareBrandBackfill(ctx); e != nil {
		t.Fatal(e)
	}
	hold, e := s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer hold.Rollback(ctx)
	if _, e = hold.Exec(ctx, `SELECT 1 FROM catalog.brands WHERE tenant_id='acme' AND id='maker-000' FOR UPDATE`); e != nil {
		t.Fatal(e)
	}
	first, e := s.BackfillBrandNames(ctx, "batch-first", 8, 10*time.Millisecond)
	if e != nil || first.Rows != 8 || !first.Pending {
		t.Fatal(first, e)
	}
	replayed, e := (&db.Store{Pool: s.Pool}).BackfillBrandNames(ctx, "batch-first", 8, 10*time.Millisecond)
	if e != nil || !replayed.Replay || replayed.Rows != first.Rows {
		t.Fatal("durable receipt missing", replayed, e)
	}
	if _, e = s.BackfillBrandNames(ctx, "batch-first", 9, 10*time.Millisecond); !errors.Is(e, db.ErrRolloutNotReady) {
		t.Fatal("changed request reused receipt", e)
	}
	for i := 0; i < 12; i++ {
		time.Sleep(12 * time.Millisecond)
		batch, e := s.BackfillBrandNames(ctx, fmt.Sprintf("batch-%02d", i), 8, 10*time.Millisecond)
		if e != nil {
			t.Fatal(e)
		}
		if batch.Rows == 0 {
			if !batch.Pending {
				t.Fatal("locked row skipped permanently")
			}
			break
		}
	}
	state, e := s.BrandRollout(ctx)
	if e != nil || state.Pending != 1 || state.Rows != 16 || state.Phase != "expanded" {
		t.Fatal(state, e)
	}
	if e = s.ValidateBrandNames(ctx, "test", "not yet drained"); !errors.Is(e, db.ErrRolloutNotReady) {
		t.Fatal("validated with pending row", e)
	}
	if e = hold.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	time.Sleep(12 * time.Millisecond)
	last, e := s.BackfillBrandNames(ctx, "batch-last", 8, 500*time.Millisecond)
	if e != nil || last.Rows != 1 || last.Pending {
		t.Fatal(last, e)
	}
	throttled, e := s.BackfillBrandNames(ctx, "batch-too-soon", 8, 500*time.Millisecond)
	if e != nil || !throttled.Throttled {
		t.Fatal("throttle did not persist", throttled, e)
	}
	var revisions int64
	if e = s.Pool.QueryRow(ctx, `SELECT sum(version) FROM catalog.brands`).Scan(&revisions); e != nil || revisions != 17 {
		t.Fatal("backfill changed business revisions", e)
	}
}
func TestCatalogEvolutionContractRequiresReaderAndRollbackRetirement(t *testing.T) {
	s := evolutionStore(t, 2)
	ctx := context.Background()
	if e := s.PrepareBrandBackfill(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := s.BackfillBrandNames(ctx, "complete", 8, 10*time.Millisecond); e != nil {
		t.Fatal(e)
	}
	if e := s.ValidateBrandNames(ctx, "test", "all tuples complete"); e != nil {
		t.Fatal(e)
	}
	if e := s.LeaseBrandReader(ctx, "legacy-test", 1, 150*time.Millisecond); e != nil {
		t.Fatal(e)
	}
	if e := s.ContractBrandNames(ctx, "test", "too early"); !errors.Is(e, db.ErrRolloutNotReady) {
		t.Fatal("contract skipped gates", e)
	}
	if e := s.OpenBrandRollbackWindow(ctx, time.Second, "test", "one-second isolated fixture"); e != nil {
		t.Fatal(e)
	}
	if e := s.RetireBrandRollback(ctx, "test", "too early"); !errors.Is(e, db.ErrRolloutNotReady) {
		t.Fatal(e)
	}
	time.Sleep(1100 * time.Millisecond)
	if e := s.RetireBrandRollback(ctx, "test", "reader not retired"); !errors.Is(e, db.ErrRolloutNotReady) {
		t.Fatal("lease expiry auto-retired reader", e)
	}
	if e := s.RetireBrandReader(ctx, "legacy-test", "test", "old process drained"); e != nil {
		t.Fatal(e)
	}
	if e := s.ContractBrandNames(ctx, "test", "rollback not retired"); !errors.Is(e, db.ErrRolloutNotReady) {
		t.Fatal(e)
	}
	if e := s.RetireBrandRollback(ctx, "test", "rollback support ended"); e != nil {
		t.Fatal(e)
	}
	if e := s.ContractBrandNames(ctx, "test", "explicit contraction"); e != nil {
		t.Fatal(e)
	}
	if e := s.LeaseBrandReader(ctx, "legacy-test", 1, time.Second); !errors.Is(e, db.ErrRolloutRetired) {
		t.Fatal("retired reader revived", e)
	}
	if _, e := s.Pool.Exec(ctx, `SELECT name FROM catalog.brands`); e == nil {
		t.Fatal("legacy column survived contract")
	}
	if got, e := s.Brand(ctx, "acme", "maker-000"); e != nil || got.Name != "Maker 000" {
		t.Fatal("current reader failed contracted schema", got, e)
	}
	if _, _, e := (describe.Handler{Store: s}).Brand(ctx, "acme", "after-contract", domain.Brand{ID: "maker-000", Name: "After contract", Version: 1}); e != nil {
		t.Fatal("current writer failed contracted schema", e)
	}
	if e := migrations.Check(ctx, s.Pool); e != nil {
		t.Fatal("source migration ledger was rewritten", e)
	}
}
func TestCatalogEvolutionConcurrentIndexRepairAndContractLockBudget(t *testing.T) {
	s := evolutionStore(t, 3)
	ctx := context.Background()
	writer, e := s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Rollback(ctx)
	if _, e = writer.Exec(ctx, `UPDATE catalog.brands SET version=version WHERE tenant_id='acme' AND id='maker-000'`); e != nil {
		t.Fatal(e)
	}
	connection, e := s.Pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = connection.Exec(ctx, `SET statement_timeout='250ms'`); e != nil {
		t.Fatal(e)
	}
	_, buildError := connection.Exec(ctx, `CREATE INDEX CONCURRENTLY brand_display_pending ON catalog.brands(tenant_id,id) WHERE display_name IS NULL`)
	connection.Exec(ctx, `SET statement_timeout='0'`)
	connection.Release()
	writer.Rollback(ctx)
	if buildError == nil {
		t.Fatal("fixture did not interrupt a concurrent index build")
	}
	var valid bool
	if e = s.Pool.QueryRow(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid='catalog.brand_display_pending'::regclass`).Scan(&valid); e != nil || valid {
		t.Fatal("interrupted build did not leave an invalid index", e)
	}
	// An invalid owned index is repaired, and repeated preparation is harmless.
	if e := s.PrepareBrandBackfill(ctx); e != nil {
		t.Fatal(e)
	}
	if e := s.PrepareBrandBackfill(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := s.BackfillBrandNames(ctx, "all", 8, 10*time.Millisecond); e != nil {
		t.Fatal(e)
	}
	if e := s.ValidateBrandNames(ctx, "test", "checked"); e != nil {
		t.Fatal(e)
	}
	if e := s.OpenBrandRollbackWindow(ctx, time.Second, "test", "fixture"); e != nil {
		t.Fatal(e)
	}
	time.Sleep(1100 * time.Millisecond)
	if e := s.RetireBrandRollback(ctx, "test", "no legacy readers"); e != nil {
		t.Fatal(e)
	}
	hold, e := s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer hold.Rollback(ctx)
	if _, e = hold.Exec(ctx, `SELECT * FROM catalog.brands`); e != nil {
		t.Fatal(e)
	}
	start := time.Now()
	if e = s.ContractBrandNames(ctx, "test", "bounded lock attempt"); e == nil {
		t.Fatal("DDL ignored held reader")
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatal("DDL wait exceeded stated test allowance", elapsed)
	}
	hold.Rollback(ctx)
	state, e := s.BrandRollout(ctx)
	if e != nil || state.Phase != "validated" {
		t.Fatal("failed contract partially published", state, e)
	}
}
