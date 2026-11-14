package sessionconsistency

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/childprocess"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/pgprocess"
)

type NativeOptions struct{ PGBin, PGShare, Workdir, Atlas string }
type Report struct {
	Profile            string             `json:"profile"`
	PostgreSQL         string             `json:"postgresql"`
	Model              []Observation      `json:"model"`
	Observations       []string           `json:"observations"`
	StaleStock         int64              `json:"stale_stock"`
	PrimaryStock       int64              `json:"primary_stock"`
	WaitMilliseconds   int64              `json:"wait_milliseconds"`
	ConcurrentSessions int                `json:"concurrent_sessions"`
	LagBeforeBytes     int64              `json:"lag_before_bytes"`
	LagAfterBytes      int64              `json:"lag_after_bytes"`
	MetricSamples      map[string]float64 `json:"metric_samples"`
}
type response struct {
	Status int
	Header http.Header
	Body   []byte
}

func request(ctx context.Context, base, key, method, path, body, receipt string, headers map[string]string) (response, error) {
	r, e := http.NewRequestWithContext(ctx, method, base+path, strings.NewReader(body))
	if e != nil {
		return response{}, e
	}
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set("Content-Type", "application/json")
	if receipt != "" {
		r.Header.Set("Idempotency-Key", receipt)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	c := &http.Client{Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, e := c.Do(r)
	if e != nil {
		return response{}, errors.New("owned Atlas HTTP endpoint unavailable")
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if e != nil || len(b) > 65536 {
		return response{}, errors.New("Atlas response bound")
	}
	return response{resp.StatusCode, resp.Header, b}, nil
}
func require(r response, status int, source string) error {
	if r.Status != status || source != "" && r.Header.Get("X-Read-Source") != source {
		return fmt.Errorf("HTTP expected %d/%s, received %d/%s: %.200s", status, source, r.Status, r.Header.Get("X-Read-Source"), r.Body)
	}
	return nil
}
func token(r response) (string, error) {
	t := r.Header.Get("X-Session-Token")
	if t == "" || len(t) > 4096 || r.Header.Get("X-Session-Status") != "issued" {
		return "", errors.New("successful observation lacks bounded session token")
	}
	return t, nil
}
func startAPI(ctx context.Context, c *pgprocess.Cluster, primary *pgprocess.Node, atlas, key, signing string, replicas map[string]*pgprocess.Node, metricsAddress ...string) (string, *childprocess.Child, error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", nil, e
	}
	addr := l.Addr().String()
	l.Close()
	dsns := map[string]string{}
	for name, n := range replicas {
		dsns[name] = c.URL(n, true)
	}
	encoded, _ := json.Marshal(dsns)
	env := map[string]string{"ATLAS_DATABASE_URL": c.URL(primary, true), "ATLAS_TENANT_KEYS": "acme=" + key + ",other=" + key + "-other", "ATLAS_HTTP_ADDR": addr, "ATLAS_READ_ROUTING_ENABLED": "true", "ATLAS_READ_TOKEN_KEY": signing, "ATLAS_READ_REPLICAS": string(encoded), "ATLAS_PROVIDER_TOKEN": strings.Repeat("p", 40), "ATLAS_GRAPHQL_CURSOR_KEY": strings.Repeat("c", 40), "ATLAS_TELEMETRY_ENABLED": "false", "ATLAS_DURABLE_TRACE": "false", "ATLAS_CACHE_BACKEND": "memory"}
	if len(metricsAddress) == 1 {
		env["ATLAS_TELEMETRY_ENABLED"] = "true"
		env["ATLAS_METRICS_ADDR"] = metricsAddress[0]
		env["ATLAS_TELEMETRY_TOKEN"] = key
	}
	child, e := childprocess.Start(ctx, atlas, c.Dir, env, "serve")
	if e != nil {
		return "", nil, e
	}
	base := "http://" + addr
	e = pgprocess.Wait(ctx, "Atlas HTTP readiness", func() (bool, error) {
		if child.Exited() {
			return false, errors.New("Atlas process exited during startup")
		}
		r, e := request(ctx, base, key, "GET", "/readyz", "", "", nil)
		return e == nil && r.Status == 200, nil
	})
	if e != nil {
		child.Stop()
		return "", nil, e
	}
	return base, child, nil
}

func RunNative(ctx context.Context, o NativeOptions) (report Report, err error) {
	report.Profile = "native-primary-two-physical-standbys"
	report.Model, err = Model()
	if err != nil {
		return report, err
	}
	if !filepath.IsAbs(o.Atlas) {
		return report, errors.New("absolute Atlas binary path required")
	}
	c, err := pgprocess.New(ctx, pgprocess.Options{Bin: o.PGBin, Share: o.PGShare, Workdir: o.Workdir})
	if err != nil {
		return report, err
	}
	defer c.Close()
	primary, err := c.Init("primary")
	if err != nil {
		return report, err
	}
	if err = c.Scalar(primary, "SHOW server_version", &report.PostgreSQL); err != nil {
		return report, err
	}
	if !strings.HasPrefix(report.PostgreSQL, "18.") {
		return report, errors.New("session drill requires PostgreSQL 18")
	}
	migrate, err := childprocess.Start(ctx, o.Atlas, c.Dir, map[string]string{"ATLAS_DATABASE_URL": c.URL(primary, true), "ATLAS_TELEMETRY_ENABLED": "false"}, "migrate")
	if err != nil {
		return report, err
	}
	defer migrate.Stop()
	if err = migrate.Wait(ctx); err != nil {
		return report, fmt.Errorf("Atlas migration: %w", err)
	}
	a, err := c.Standby(primary, "replicaa")
	if err != nil {
		return report, err
	}
	b, err := c.Standby(primary, "replicab")
	if err != nil {
		return report, err
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return report, err
	}
	key := hex.EncodeToString(secret)
	signing := key + "-read-signing"
	left, pLeft, err := startAPI(ctx, c, primary, o.Atlas, key, signing, map[string]*pgprocess.Node{"replica-a": a, "replica-b": b})
	if err != nil {
		return report, err
	}
	defer pLeft.Stop()
	ml, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return report, e
	}
	metricsAddress := ml.Addr().String()
	ml.Close()
	right, pRight, err := startAPI(ctx, c, primary, o.Atlas, key, signing, map[string]*pgprocess.Node{"replica-b": b}, metricsAddress)
	if err != nil {
		return report, err
	}
	defer pRight.Stop()
	call := func(base, method, path, body, receipt string, h map[string]string) (response, error) {
		return request(ctx, base, key, method, path, body, receipt, h)
	}
	get := func(base, path, t, mode, lag, wait string) (response, error) {
		return call(base, "GET", path, "", "", map[string]string{"X-Session-Token": t, "X-Read-Consistency": mode, "X-Read-On-Lag": lag, "X-Read-Wait-Ms": wait})
	}
	path := "/v1/stock/BOOK"
	r, err := call(left, "POST", path+"/receipts", `{"quantity":10}`, "seed", nil)
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "primary"); err != nil {
		return report, err
	}
	if err = c.Catchup(primary, a); err != nil {
		return report, err
	}
	if err = c.Catchup(primary, b); err != nil {
		return report, err
	}
	pause := func(n *pgprocess.Node) error {
		if err := c.Exec(n, "SELECT pg_wal_replay_pause()"); err != nil {
			return err
		}
		return pgprocess.Wait(ctx, "replay pause", func() (bool, error) {
			var paused bool
			e := c.Scalar(n, "SELECT pg_get_wal_replay_pause_state()='paused'", &paused)
			return paused, e
		})
	}
	for _, n := range []*pgprocess.Node{a, b} {
		if err = pause(n); err != nil {
			return report, err
		}
	}

	r, err = call(left, "POST", path+"/receipts", `{"quantity":10}`, "increase", nil)
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "primary"); err != nil {
		return report, err
	}
	writeToken, err := token(r)
	if err != nil {
		return report, err
	}
	savedBody := append([]byte(nil), r.Body...)
	if err = c.Scalar(b, "SELECT on_hand FROM inventory.stock WHERE tenant_id='acme' AND sku='BOOK'", &report.StaleStock); err != nil {
		return report, err
	}
	if err = c.Scalar(primary, "SELECT on_hand FROM inventory.stock WHERE tenant_id='acme' AND sku='BOOK'", &report.PrimaryStock); err != nil {
		return report, err
	}
	var walFence, replayed string
	if err = c.Scalar(primary, "SELECT pg_current_wal_insert_lsn()::text", &walFence); err != nil {
		return report, err
	}
	if err = c.Scalar(b, "SELECT pg_last_wal_replay_lsn()::text", &replayed); err != nil {
		return report, err
	}
	if err = c.Scalar(primary, "SELECT GREATEST(0,pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn))::bigint", &report.LagBeforeBytes, walFence, replayed); err != nil {
		return report, err
	}
	if report.LagBeforeBytes <= 0 {
		return report, errors.New("native WAL lag measurement did not expose staleness")
	}
	if report.StaleStock != 10 || report.PrimaryStock != 20 {
		return report, errors.New("stale read anomaly not reproduced")
	}
	r, err = get(right, path, writeToken, "replica", "pending", "20")
	if err != nil {
		return report, err
	}
	if err = require(r, 425, ""); err != nil {
		return report, err
	}
	if r.Header.Get("Retry-After") == "" || r.Header.Get("X-Session-Token") != "" {
		return report, errors.New("pending response falsely advanced session")
	}
	r, err = get(right, path, writeToken, "replica", "primary", "20")
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "primary"); err != nil {
		return report, err
	}
	if r.Header.Get("X-Read-Reason") != "replica-timeout-primary" {
		return report, errors.New("unlabeled fallback")
	}
	metricSample := func() (map[string]float64, error) {
		m, e := request(ctx, "http://"+metricsAddress, key, "GET", "/metrics", "", "", nil)
		if e != nil {
			return nil, e
		}
		if e = require(m, 200, ""); e != nil {
			return nil, e
		}
		out := map[string]float64{}
		for _, line := range strings.Split(string(m.Body), "\n") {
			if !strings.HasPrefix(line, "atlas_read_") {
				continue
			}
			parts := strings.Fields(line)
			if len(parts) == 2 {
				n, e := strconv.ParseFloat(parts[1], 64)
				if e != nil {
					return nil, e
				}
				out[parts[0]] = n
			}
		}
		if bytes.Contains(m.Body, []byte(key)) || bytes.Contains(m.Body, []byte("postgresql://")) || bytes.Contains(m.Body, []byte(writeToken)) {
			return nil, errors.New("routing metrics leaked authority/session material")
		}
		return out, nil
	}
	report.MetricSamples, err = metricSample()
	if err != nil {
		return report, err
	}
	gapName := `atlas_read_replica_fence_gap_bytes{replica="replica-b"}`
	if report.MetricSamples[gapName] <= 0 || report.MetricSamples["atlas_read_pending_total"] < 1 || report.MetricSamples["atlas_read_primary_fallback_total"] < 1 {
		return report, errors.New("private routing metrics did not report gap/pending/fallback")
	}
	report.Observations = append(report.Observations, "acknowledged write is absent on paused standby; token yields 425 or explicit primary fallback")
	r, err = call(left, "POST", path+"/receipts", `{"quantity":10}`, "increase", map[string]string{"X-Session-Token": writeToken})
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "primary"); err != nil {
		return report, err
	}
	if !bytes.Equal(r.Body, savedBody) || r.Header.Get("Idempotent-Replay") != "true" {
		return report, errors.New("session fence altered saved command receipt")
	}
	// Learn a fast replica result; switch to an API where its sticky endpoint
	// is absent. The slower replica cannot satisfy the learned watermark.
	if err = c.Exec(a, "SELECT pg_wal_replay_resume()"); err != nil {
		return report, err
	}
	if err = c.Catchup(primary, a); err != nil {
		return report, err
	}
	r, err = get(left, path, writeToken, "replica", "pending", "1000")
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "replica-a"); err != nil {
		return report, err
	}
	seenToken, err := token(r)
	if err != nil {
		return report, err
	}
	var fast, slow int64
	if err = c.Scalar(a, "SELECT version FROM inventory.stock WHERE tenant_id='acme' AND sku='BOOK'", &fast); err != nil {
		return report, err
	}
	if err = c.Scalar(b, "SELECT version FROM inventory.stock WHERE tenant_id='acme' AND sku='BOOK'", &slow); err != nil {
		return report, err
	}
	if fast <= slow {
		return report, errors.New("raw alternating reads did not regress")
	}
	r, err = get(right, path, seenToken, "replica", "pending", "10")
	if err != nil {
		return report, err
	}
	if err = require(r, 425, ""); err != nil {
		return report, err
	}
	report.Observations = append(report.Observations, "raw replicas regress from version 2 to 1; a retained session fence survives unavailable sticky preference")
	// Resume inside the bounded wait, rather than merely exercising timeout.
	resumed := make(chan error, 1)
	go func() {
		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			resumed <- ctx.Err()
		case <-timer.C:
			resumed <- c.Exec(b, "SELECT pg_wal_replay_resume()")
		}
	}()
	started := time.Now()
	r, err = get(right, path, seenToken, "replica", "pending", "1500")
	report.WaitMilliseconds = time.Since(started).Milliseconds()
	resumeErr := <-resumed
	if err != nil {
		return report, err
	}
	if resumeErr != nil {
		return report, resumeErr
	}
	if err = require(r, 200, "replica-b"); err != nil {
		return report, err
	}
	if report.WaitMilliseconds < 80 || report.WaitMilliseconds > 2200 {
		return report, errors.New("replica wait did not respect delayed replay/budget")
	}
	if err = c.Scalar(b, "SELECT GREATEST(0,pg_wal_lsn_diff($1::pg_lsn,pg_last_wal_replay_lsn()))::bigint", &report.LagAfterBytes, walFence); err != nil {
		return report, err
	}
	if report.LagAfterBytes != 0 {
		return report, errors.New("WAL gap stayed positive after bounded replay")
	}
	afterMetrics, e := metricSample()
	if e != nil {
		return report, e
	}
	if afterMetrics[gapName] != 0 || afterMetrics[`atlas_read_replica_probe_up{replica="replica-b"}`] != 1 {
		return report, errors.New("routing probe metrics did not recover")
	}
	report.Observations = append(report.Observations, "measured primary/replay byte gap and private request-driven fence gap are positive during lag and zero after replay; pending/fallback counters advance without secret labels")
	current, err := token(r)
	if err != nil {
		return report, err
	}
	report.Observations = append(report.Observations, "bounded wait observes delayed WAL replay and relocates the sticky hint to replica-b")
	// Freeze old stock on B. Reserve more than its observed on_hand on the
	// primary and read that dependent reservation from A to show mixed prefix.
	if err = c.Catchup(primary, b); err != nil {
		return report, err
	}
	if err = pause(b); err != nil {
		return report, err
	}
	r, err = call(left, "POST", "/v1/reservations", `{"sku":"BOOK","quantity":15,"ttl_seconds":3600}`, "reserve", map[string]string{"X-Session-Token": current})
	if err != nil {
		return report, err
	}
	if err = require(r, 201, "primary"); err != nil {
		return report, err
	}
	var receipt struct {
		Reservation struct {
			ID string `json:"id"`
		} `json:"reservation"`
	}
	if err = json.Unmarshal(r.Body, &receipt); err != nil {
		return report, err
	}
	reservationID := receipt.Reservation.ID
	if reservationID == "" {
		return report, errors.New("reservation receipt ID missing")
	}
	reserveToken, err := token(r)
	if err != nil {
		return report, err
	}
	if err = c.Catchup(primary, a); err != nil {
		return report, err
	}
	// BOOK already has 20 on B; use its stale reserved=0 versus the live
	// dependent reservation=15 to expose a torn stock/reservation view.
	var staleReserved, childQuantity int64
	if err = c.Scalar(b, "SELECT reserved FROM inventory.stock WHERE tenant_id='acme' AND sku='BOOK'", &staleReserved); err != nil {
		return report, err
	}
	if err = c.Scalar(a, "SELECT quantity FROM inventory.reservations WHERE id=$1::uuid", &childQuantity, reservationID); err != nil {
		return report, err
	}
	if staleReserved != 0 || childQuantity != 15 {
		return report, errors.New("mixed prefix anomaly missing")
	}
	snapshot := "/v1/inventory/snapshot?sku=BOOK&reservation_id=" + reservationID
	r, err = get(left, snapshot, reserveToken, "replica", "pending", "1000")
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "replica-a"); err != nil {
		return report, err
	}
	var view struct {
		Stock struct {
			Reserved int64 `json:"reserved"`
		} `json:"stock"`
		Reservation struct {
			Quantity int64 `json:"quantity"`
		} `json:"reservation"`
	}
	if err = json.Unmarshal(r.Body, &view); err != nil {
		return report, err
	}
	if view.Stock.Reserved != 15 || view.Reservation.Quantity != 15 {
		return report, errors.New("snapshot returned torn prefix")
	}
	current, err = token(r)
	if err != nil {
		return report, err
	}
	report.Observations = append(report.Observations, "mixed replicas return reserved=0 with a reservation of 15; combined RR snapshot returns reserved=15 and quantity=15")
	// Invalid session input must not execute a write or leak a different tenant.
	bad := current
	replacement := "A"
	if bad[0] == 'A' {
		replacement = "B"
	}
	bad = replacement + bad[1:]
	r, err = call(left, "POST", path+"/receipts", `{"quantity":1}`, "tampered", map[string]string{"X-Session-Token": bad})
	if err != nil {
		return report, err
	}
	if err = require(r, 400, ""); err != nil {
		return report, err
	}
	r, err = request(ctx, left, key+"-other", "GET", path, "", "", map[string]string{"X-Session-Token": current})
	if err != nil {
		return report, err
	}
	if err = require(r, 400, ""); err != nil {
		return report, err
	}
	r, err = get(left, path+"/projection", current, "primary", "pending", "0")
	if err != nil {
		return report, err
	}
	if err = require(r, 400, ""); err != nil {
		return report, err
	}
	r, err = get(left, "/v1/reservations/"+reservationID, current, "primary", "pending", "0")
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "primary"); err != nil {
		return report, err
	}
	r, err = get(left, "/v1/stock/UNKNOWN", current, "primary", "pending", "0")
	if err != nil {
		return report, err
	}
	if err = require(r, 404, "primary"); err != nil {
		return report, err
	}
	if _, err = token(r); err != nil {
		return report, err
	}
	report.Observations = append(report.Observations, "tampered and cross-tenant tokens rejected; async projection contract rejected; reservation and authoritative absence carry fences")
	// Same signed sticky token, a dead A and paused B: policy remains explicit.
	c.Stop(a)
	r, err = get(left, path, current, "replica", "pending", "20")
	if err != nil {
		return report, err
	}
	if err = require(r, 425, ""); err != nil {
		return report, err
	}
	r, err = get(left, path, current, "replica", "primary", "20")
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "primary"); err != nil {
		return report, err
	}
	if err = c.Start(a); err != nil {
		return report, err
	}
	if err = c.Exec(b, "SELECT pg_wal_replay_resume()"); err != nil {
		return report, err
	}
	if err = c.Catchup(primary, a); err != nil {
		return report, err
	}
	if err = c.Catchup(primary, b); err != nil {
		return report, err
	}
	report.Observations = append(report.Observations, "stopped sticky endpoint plus lagging alternative produces bounded pending/fallback, then recovers")
	// Independent tenant/client requests run concurrently; no server session
	// registry or per-tenant global mutex is needed to track their watermarks.
	var wg sync.WaitGroup
	failures := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bearer := key
			if i%2 == 1 {
				bearer += "-other"
			}
			r, e := request(ctx, left, bearer, "GET", path, "", "", map[string]string{"X-Read-Consistency": "primary"})
			if e == nil {
				expected := 200
				if i%2 == 1 {
					expected = 404
				}
				e = require(r, expected, "primary")
				if e == nil {
					_, e = token(r)
				}
			}
			failures <- e
		}(i)
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		if e != nil {
			return report, e
		}
	}
	report.ConcurrentSessions = 12
	wrongRole, pWrong, err := startAPI(ctx, c, primary, o.Atlas, key, signing, map[string]*pgprocess.Node{"wrong-role": primary})
	if err != nil {
		return report, err
	}
	defer pWrong.Stop()
	r, err = get(wrongRole, path, current, "replica", "pending", "0")
	if err != nil {
		return report, err
	}
	if err = require(r, 425, ""); err != nil {
		return report, err
	}
	report.Observations = append(report.Observations, "primary misconfigured as a replica is excluded by the live recovery-role check")
	// History is changed by the migration/operator authority. Existing API
	// incarnations stop issuing fences; new incarnations reject old tokens.
	if err = pause(b); err != nil {
		return report, err
	}
	if err = c.Exec(primary, "UPDATE platform.writer_fence SET epoch=epoch+1"); err != nil {
		return report, err
	}
	r, err = get(left, path, current, "primary", "pending", "0")
	if err != nil {
		return report, err
	}
	if err = require(r, 409, ""); err != nil {
		return report, err
	}
	fresh, pFresh, err := startAPI(ctx, c, primary, o.Atlas, key, signing, map[string]*pgprocess.Node{"replica-b": b})
	if err != nil {
		return report, err
	}
	defer pFresh.Stop()
	r, err = get(fresh, path, current, "primary", "pending", "0")
	if err != nil {
		return report, err
	}
	if err = require(r, 409, ""); err != nil {
		return report, err
	}
	r, err = get(fresh, path, "", "replica", "pending", "10")
	if err != nil {
		return report, err
	}
	if err = require(r, 425, ""); err != nil {
		return report, err
	}
	if err = c.Exec(b, "SELECT pg_wal_replay_resume()"); err != nil {
		return report, err
	}
	if err = c.Catchup(primary, b); err != nil {
		return report, err
	}
	r, err = get(fresh, path, "", "replica", "pending", "1000")
	if err != nil {
		return report, err
	}
	if err = require(r, 200, "replica-b"); err != nil {
		return report, err
	}
	report.Observations = append(report.Observations, "epoch change stops old API fences and old tokens; replica from prior history excluded until replay catches up")
	var actual int64
	if err = c.Scalar(primary, "SELECT on_hand FROM inventory.stock WHERE tenant_id='acme' AND sku='BOOK'", &actual); err != nil {
		return report, err
	}
	if actual != 20 {
		return report, errors.New("invalid input or idempotency replay changed business effect")
	}
	return report, nil
}

func Save(path string, r Report) error {
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}
