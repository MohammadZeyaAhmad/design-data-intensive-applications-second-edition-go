package singleleader

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

func (f *fixture) configureSync(mode Mode) error {
	value := "on"
	names := "FIRST 1 (atlas_replica)"
	if mode == Async {
		names = ""
	}
	if mode == RemoteApply {
		value = "remote_apply"
	}
	if err := f.exec(f.a, "ALTER SYSTEM SET synchronous_standby_names="+literal(names)); err != nil {
		return err
	}
	if err := f.exec(f.a, "ALTER SYSTEM SET synchronous_commit="+literal(value)); err != nil {
		return err
	}
	return f.exec(f.a, "SELECT pg_reload_conf()")
}
func (f *fixture) disconnectReceiver() error {
	port, err := freePort()
	if err != nil {
		return err
	}
	conninfo := "host=127.0.0.1 port=" + strconv.Itoa(port) + " user=atlas_repl connect_timeout=1 application_name=atlas_replica"
	if err = f.exec(f.b, "ALTER SYSTEM SET primary_conninfo="+literal(conninfo)); err != nil {
		return err
	}
	if err = f.exec(f.b, "SELECT pg_reload_conf()"); err != nil {
		return err
	}
	return wait(f.ctx, "WAL transport disconnected", func() (bool, error) {
		var n int
		err := f.scalar(f.a, "SELECT count(*) FROM pg_stat_replication", &n)
		return n == 0, err
	})
}
func (f *fixture) reconnectReceiver() error {
	if err := f.exec(f.b, "ALTER SYSTEM SET primary_conninfo="+literal(f.url(f.a, "atlas_repl", false))); err != nil {
		return err
	}
	if err := f.exec(f.b, "SELECT pg_reload_conf()"); err != nil {
		return err
	}
	return f.catchup(f.a, f.b)
}
func (f *fixture) pause() error {
	if err := f.exec(f.b, "SELECT pg_wal_replay_pause()"); err != nil {
		return err
	}
	return wait(f.ctx, "standby replay paused", func() (bool, error) {
		var b bool
		err := f.scalar(f.b, "SELECT pg_is_wal_replay_paused()", &b)
		return b, err
	})
}
func (f *fixture) resume() error { return f.exec(f.b, "SELECT pg_wal_replay_resume()") }
func (f *fixture) waitSync() error {
	return wait(f.ctx, "actual synchronous commit wait", func() (bool, error) {
		var n int
		err := f.scalar(f.a, "SELECT count(*) FROM pg_stat_activity WHERE application_name='atlas-http/local' AND wait_event='SyncRep'", &n)
		return n > 0, err
	})
}

// waitLSN is an explicit read contract for one follower. It never routes a
// freshness-sensitive read until replay reaches the primary's position.
func (f *fixture) waitLSN(ctx context.Context, n *node, lsn string) error {
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		if err := f.scalar(n, "SELECT coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn,false)", &ready, lsn); err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func runMode(ctx context.Context, o NativeOptions, mode Mode, r *NativeReport) error {
	f, err := newFixture(ctx, o)
	if err != nil {
		return err
	}
	defer f.close()
	api1, err := f.api(f.a, 1)
	if err != nil {
		return err
	}
	if err = api1.receipt(ctx, "snapshot-seed"); err != nil {
		return err
	}
	if err = f.bootstrap(); err != nil {
		return err
	}
	count, err := f.receiptCount(f.b, "snapshot-seed")
	if err != nil || count != 1 {
		return errors.New("consistent follower snapshot lost seed receipt")
	}
	var readonly bool
	if err = f.scalar(f.b, "SELECT pg_is_in_recovery() AND current_setting('transaction_read_only')='on'", &readonly); err != nil || !readonly {
		return errors.New("follower is not read-only")
	}
	if mode == Async {
		f.b.proc.stop(false)
		if err = api1.receipt(ctx, "during-follower-outage"); err != nil {
			return err
		}
		if err = f.start(f.b); err != nil {
			return err
		}
		if err = f.catchup(f.a, f.b); err != nil {
			return err
		}
		count, err = f.receiptCount(f.b, "during-follower-outage")
		if err != nil || count != 1 {
			return errors.New("follower outage catch-up lost receipt")
		}
		r.record("snapshot_plus_slot_catchup", true)
	}
	if err = f.configureSync(mode); err != nil {
		return err
	}
	if mode == RemoteFlush {
		// Cancel a client while PostgreSQL is waiting for remote durability.
		// Its transaction may already be committed locally. No ACK is counted.
		if err = f.disconnectReceiver(); err != nil {
			return err
		}
		requestCtx, cancel := context.WithCancel(ctx)
		finished := make(chan error, 1)
		go func() { finished <- api1.receipt(requestCtx, "uncertain-sync") }()
		if err = f.waitSync(); err != nil {
			cancel()
			<-finished
			return err
		}
		cancel()
		if err = <-finished; err == nil {
			return errors.New("disconnected standby returned acknowledgment")
		}
		if err = wait(ctx, "uncertain commit locally visible", func() (bool, error) { n, e := f.receiptCount(f.a, "uncertain-sync"); return n == 1, e }); err != nil {
			return err
		}
		count, err = f.receiptCount(f.b, "uncertain-sync")
		if err != nil || count != 0 {
			return errors.New("disconnected follower unexpectedly has uncertain commit")
		}
		if err = f.reconnectReceiver(); err != nil {
			return err
		}
		before, err := f.receiptCount(f.a, "uncertain-sync")
		if err != nil {
			return err
		}
		if err = api1.receipt(ctx, "uncertain-sync"); err != nil {
			return err
		}
		after, err := f.receiptCount(f.a, "uncertain-sync")
		if err != nil || before != 1 || after != 1 {
			return errors.New("uncertain retry duplicated effect")
		}
		r.record("synchronous_timeout_is_uncertain_and_intent_reconciles_once", true)
	}
	if mode == Async {
		err = f.disconnectReceiver()
	} else {
		err = f.pause()
	}
	if err != nil {
		return err
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- api1.receipt(ctx, "durability-probe") }()
	if mode == RemoteApply {
		if err = f.waitSync(); err != nil {
			return err
		}
		select {
		case err = <-done:
			return fmt.Errorf("remote_apply replied before replay: %v", err)
		default:
		}
		count, err = f.receiptCount(f.b, "durability-probe")
		if err != nil || count != 0 {
			return errors.New("paused replica unexpectedly applied command")
		}
		r.record("remote_apply_withholds_reply_until_visibility", true)
		if err = f.resume(); err != nil {
			return err
		}
	}
	if err = <-done; err != nil {
		return err
	}
	elapsed := time.Since(start).Milliseconds()
	evidence, err := f.lags()
	if err != nil {
		return err
	}
	evidence.Mode = mode
	evidence.Acknowledged = 1
	evidence.ReplyMillis = elapsed
	count, err = f.receiptCount(f.b, "durability-probe")
	if err != nil {
		return err
	}
	if mode == RemoteFlush {
		if count != 0 || evidence.ReplayLagBytes <= 0 {
			return errors.New("remote flush did not exhibit stale replay")
		}
		gate, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		err = f.waitLSN(gate, f.b, evidence.PrimaryLSN)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			return errors.New("fresh read gate accepted paused replica")
		}
		r.record("remote_flush_ack_is_not_replica_read_visibility", true)
	}
	if mode == Async && (count != 0 || evidence.ReceiveLagBytes <= 0) {
		return errors.New("async transport failure did not expose acknowledged lag")
	}
	if mode == RemoteApply && count != 1 {
		return errors.New("remote apply reply preceded visibility")
	}
	// The old API remains live to prove that a fenced database cannot accept
	// another write. The physical database is stopped before promotion.
	if err = f.fence(f.a); err != nil {
		return err
	}
	code, _, requestErr := api1.request(ctx, "POST", "/v1/stock/BOOK/receipts", "old-primary-after-fence", `{"quantity":1}`)
	if requestErr == nil && code == 200 {
		return errors.New("old primary accepted a post-fence write")
	}
	api1.proc.stop(false)
	if mode == RemoteFlush {
		if err = f.resume(); err != nil {
			return err
		}
		gate, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = f.waitLSN(gate, f.b, evidence.PrimaryLSN)
		cancel()
		if err != nil {
			return err
		}
		r.record("replica_lsn_read_gate_succeeds_after_replay", true)
	}
	if err = f.promote(f.b, 2); err != nil {
		return err
	}
	count, err = f.receiptCount(f.b, "durability-probe")
	if err != nil {
		return err
	}
	evidence.LostAcknowledged = 1 - count
	if (mode == Async && evidence.LostAcknowledged != 1) || (mode != Async && evidence.LostAcknowledged != 0) {
		return errors.New("unexpected acknowledged-write loss")
	}
	r.Modes = append(r.Modes, evidence)
	stale, err := f.api(f.b, 1)
	if err != nil {
		return err
	}
	code, _, err = stale.request(ctx, "POST", "/v1/stock/BOOK/receipts", "stale-process", `{"quantity":1}`)
	if err != nil {
		return err
	}
	if code == 200 {
		return errors.New("stale writer token admitted")
	}
	stale.proc.stop(false)
	count, err = f.receiptCount(f.b, "stale-process")
	if err != nil || count != 0 {
		return errors.New("stale writer left receipt")
	}
	current, err := f.api(f.b, 2)
	if err != nil {
		return err
	}
	if err = current.receipt(ctx, "new-epoch"); err != nil {
		return err
	}
	r.record(string(mode)+"_fenced_promotion_and_inventory_epoch_rejection", true)
	if mode == Async {
		if err = f.rewind(); err != nil {
			return err
		}
		r.record("divergent_old_primary_pg_rewind_and_read_only_rejoin", true)
	}
	return nil
}
func (f *fixture) rewind() error {
	if err := f.exec(f.b, "CHECKPOINT"); err != nil {
		return err
	}
	if err := f.command("pg_rewind", "--target-pgdata", f.a.dir, "--source-server", f.url(f.b, "atlas_admin", false), "--write-recovery-conf"); err != nil {
		return err
	}
	if err := f.exec(f.b, "SELECT pg_create_physical_replication_slot('atlas_rejoin')"); err != nil {
		return err
	}
	conf := "\nprimary_conninfo=" + literal(f.url(f.b, "atlas_repl", false)) + "\nprimary_slot_name='atlas_rejoin'\narchive_mode=off\nsynchronous_standby_names=''\n"
	if err := appendConfig(f.a.dir, conf); err != nil {
		return err
	}
	if err := f.start(f.a); err != nil {
		return err
	}
	if err := f.catchup(f.b, f.a); err != nil {
		return err
	}
	var ro bool
	if err := f.scalar(f.a, "SELECT pg_is_in_recovery()", &ro); err != nil || !ro {
		return errors.New("rewound old primary not read-only")
	}
	n, err := f.receiptCount(f.a, "durability-probe")
	if err != nil || n != 0 {
		return errors.New("divergent acknowledged branch was merged")
	}
	n, err = f.receiptCount(f.a, "new-epoch")
	if err != nil || n != 1 {
		return errors.New("rewound follower missed new leader")
	}
	return nil
}
