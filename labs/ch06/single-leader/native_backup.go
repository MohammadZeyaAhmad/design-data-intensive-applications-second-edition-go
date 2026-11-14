package singleleader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type inventoryState struct {
	OnHand            int64 `json:"on_hand"`
	Reserved          int64 `json:"reserved"`
	Allocated         int64 `json:"allocated"`
	Version           int64 `json:"version"`
	Receipts          int   `json:"receipts"`
	Events            int   `json:"events"`
	ActiveQuantity    int64 `json:"active_reservation_quantity"`
	CommittedQuantity int64 `json:"committed_reservation_quantity"`
}

func (f *fixture) state(n *node) (inventoryState, error) {
	c, err := f.connect(n)
	if err != nil {
		return inventoryState{}, err
	}
	defer c.Close(context.Background())
	var s inventoryState
	err = c.QueryRow(f.ctx, `SELECT on_hand,reserved,allocated,version,
(SELECT count(*) FROM inventory.command_results WHERE tenant_id='replication'),
(SELECT count(*) FROM inventory.outbox WHERE tenant_id='replication'),
(SELECT coalesce(sum(quantity) FILTER(WHERE status='active'),0) FROM inventory.reservations WHERE tenant_id='replication'),
(SELECT coalesce(sum(quantity) FILTER(WHERE status='committed'),0) FROM inventory.reservations WHERE tenant_id='replication')
FROM inventory.stock WHERE tenant_id='replication' AND sku='BOOK' AND warehouse_id='default'`).Scan(&s.OnHand, &s.Reserved, &s.Allocated, &s.Version, &s.Receipts, &s.Events, &s.ActiveQuantity, &s.CommittedQuantity)
	if err == nil && (s.OnHand < 0 || s.Reserved < 0 || s.Allocated < 0 || s.Reserved+s.Allocated > s.OnHand || s.Events != s.Receipts || int64(s.Events) != s.Version || s.Reserved != s.ActiveQuantity || s.Allocated != s.CommittedQuantity) {
		return s, errors.New("Inventory stock/event/receipt invariants failed")
	}
	return s, err
}
func copyTree(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(dest, 0700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("backup contains unsupported special file")
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, in)
		if err == nil {
			err = out.Sync()
		}
		closeErr := out.Close()
		if err != nil {
			return err
		}
		return closeErr
	})
}
func runBackup(ctx context.Context, o NativeOptions, r *NativeReport) error {
	f, err := newFixture(ctx, o)
	if err != nil {
		return err
	}
	defer f.close()
	a, err := f.api(f.a, 1)
	if err != nil {
		return err
	}
	if err = a.receipt(ctx, "backup-seed"); err != nil {
		return err
	}
	if err = f.bootstrap(); err != nil {
		return err
	}
	backup := filepath.Join(f.dir, "base-backup")
	completed := make(chan error, 1)
	go func() {
		completed <- f.command("pg_basebackup", "-d", f.url(f.a, "atlas_repl", false), "-D", backup, "-X", "stream", "--checkpoint=fast", "--manifest-checksums=SHA256", "--max-rate=4096")
	}()
	if err = wait(ctx, "base backup in progress", func() (bool, error) {
		select {
		case e := <-completed:
			if e == nil {
				e = errors.New("backup completed before concurrent workload")
			}
			return false, e
		default:
		}
		var n int
		e := f.scalar(f.a, "SELECT count(*) FROM pg_stat_progress_basebackup", &n)
		return n > 0, e
	}); err != nil {
		return err
	}
	for i := range 16 {
		if err = a.receipt(ctx, fmt.Sprintf("during-basebackup-%02d", i)); err != nil {
			return err
		}
	}
	if err = <-completed; err != nil {
		return err
	}
	if err = f.command("pg_verifybackup", backup); err != nil {
		return err
	}
	r.record("real_basebackup_overlaps_16_atlas_writes_and_manifest_verifies", true)
	var relative string
	if err = f.scalar(f.a, "SELECT pg_relation_filepath('inventory.stock')", &relative); err != nil {
		return err
	}
	pagePath := filepath.Join(backup, relative)
	original, err := os.ReadFile(pagePath)
	if err != nil || len(original) == 0 {
		return errors.New("backup Inventory relation missing")
	}
	changed := append([]byte(nil), original...)
	changed[len(changed)/2] ^= 0x80
	if err = os.WriteFile(pagePath, changed, 0600); err != nil {
		return err
	}
	verificationErr := f.command("pg_verifybackup", backup)
	if err = os.WriteFile(pagePath, original, 0600); err != nil {
		return err
	}
	if verificationErr == nil {
		return errors.New("corrupted backup passed checksum verification")
	}
	if err = f.command("pg_verifybackup", backup); err != nil {
		return err
	}
	r.record("backup_corruption_is_rejected_before_restore", true)
	if err = a.receipt(ctx, "before-restore-point"); err != nil {
		return err
	}
	for _, quantity := range []int{2, 1} {
		code, body, e := a.request(ctx, "POST", "/v1/reservations", fmt.Sprintf("backup-reserve-%d", quantity), fmt.Sprintf(`{"sku":"BOOK","quantity":%d}`, quantity))
		if e != nil || code != 201 {
			return fmt.Errorf("backup reservation: HTTP %d", code)
		}
		var out struct {
			Reservation struct {
				ID string `json:"id"`
			} `json:"reservation"`
		}
		if json.Unmarshal(body, &out) != nil || out.Reservation.ID == "" {
			return errors.New("backup reservation identity missing")
		}
		if quantity == 1 {
			code, _, e = a.request(ctx, "POST", "/v1/reservations/"+out.Reservation.ID+"/commit", "backup-allocation", "")
			if e != nil || code != 200 {
				return errors.New("backup allocation failed")
			}
		}
	}
	expected, err := f.state(f.a)
	if err != nil {
		return err
	}
	var targetLSN string
	if err = f.scalar(f.a, "SELECT pg_create_restore_point('atlas_before_operator_error')::text", &targetLSN); err != nil {
		return err
	}
	if err = a.receipt(ctx, "after-restore-point"); err != nil {
		return err
	}
	if err = f.exec(f.a, "DROP TABLE inventory.stock CASCADE"); err != nil {
		return err
	}
	if err = f.catchup(f.a, f.b); err != nil {
		return err
	}
	for _, n := range []*node{f.a, f.b} {
		var missing bool
		if err = f.scalar(n, "SELECT to_regclass('inventory.stock') IS NULL", &missing); err != nil || !missing {
			return errors.New("logical operator error did not replicate")
		}
	}
	r.record("logical_stock_table_loss_replicates_to_standby", true)
	var archiveSegment string
	if err = f.scalar(f.a, "SELECT pg_walfile_name(pg_current_wal_lsn())", &archiveSegment); err != nil {
		return err
	}
	if err = f.exec(f.a, "SELECT pg_switch_wal()"); err != nil {
		return err
	}
	if err = wait(ctx, "complete archived WAL", func() (bool, error) {
		_, e := os.Stat(filepath.Join(f.archive, archiveSegment))
		if os.IsNotExist(e) {
			return false, nil
		}
		return e == nil, e
	}); err != nil {
		return err
	}
	start := time.Now()
	port, err := freePort()
	if err != nil {
		return err
	}
	restored := &node{dir: filepath.Join(f.dir, "restored"), port: port}
	if err = copyTree(backup, restored.dir); err != nil {
		return err
	}
	restoreCommand := shell(o.Helper) + " wal-restore " + shell(f.archive) + " '%f' '%p'"
	conf := "\narchive_mode=off\nprimary_conninfo=''\nprimary_slot_name=''\nsynchronous_standby_names=''\nrestore_command=" + literal(restoreCommand) + "\nrecovery_target_name='atlas_before_operator_error'\nrecovery_target_action='promote'\n"
	if err = appendConfig(restored.dir, conf); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(restored.dir, "recovery.signal"), nil, 0600); err != nil {
		return err
	}
	if err = f.start(restored); err != nil {
		return err
	}
	if err = wait(ctx, "PITR target promotion", func() (bool, error) {
		var recovery bool
		e := f.scalar(restored, "SELECT pg_is_in_recovery()", &recovery)
		return !recovery, e
	}); err != nil {
		return err
	}
	actual, err := f.state(restored)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("PITR state mismatch: got %+v want %+v", actual, expected)
	}
	before, err := f.receiptCount(restored, "before-restore-point")
	if err != nil {
		return err
	}
	after, err := f.receiptCount(restored, "after-restore-point")
	if err != nil || before != 1 || after != 0 {
		return errors.New("PITR crossed named recovery target")
	}
	restoredAPI, err := f.api(restored, 1)
	if err != nil {
		return err
	}
	code, body, err := restoredAPI.request(ctx, "GET", "/v1/stock/BOOK", "", "")
	var httpState inventoryState
	if err != nil || code != 200 || json.Unmarshal(body, &httpState) != nil || httpState.OnHand != expected.OnHand || httpState.Reserved != expected.Reserved || httpState.Allocated != expected.Allocated || httpState.Version != expected.Version {
		return errors.New("restored real Atlas read did not match invariant state")
	}
	r.record("pitr_restore_target_lsn", targetLSN)
	r.record("pitr_independent_restore_matches_stock_receipts_events", true)
	r.record("pitr_local_recovery_millis", time.Since(start).Milliseconds())
	r.record("pitr_deliberately_excluded_acknowledged_writes", 1)
	r.record("pitr_restored_inventory_state", actual)
	return nil
}
