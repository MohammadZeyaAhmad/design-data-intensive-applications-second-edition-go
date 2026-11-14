//go:build integration && observe

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/telemetry"
)

func TestObservabilityRealPrometheusAlertsAndSLIs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	tools := os.Getenv("ATLAS_OPERATIONS_TOOLS")
	if tools == "" {
		tools = filepath.Join(os.TempDir(), "atlas-operations-tools")
	}
	binary := filepath.Join(tools, "prometheus")
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("pinned Prometheus is required; run make observability-tools")
	}
	runtime, err := telemetry.New(telemetry.Options{Enabled: true, Service: "api", Version: "development", Sample: 1, Token: "native-prometheus-fixture-credential-32-characters"})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(ctx)
	endpoint := httptest.NewServer(runtime.MetricsHandler())
	defer endpoint.Close()
	dir := t.TempDir()
	rules, err := filepath.Abs("../../deploy/observe/rules.yaml")
	if err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("global:\n  scrape_interval: 200ms\n  evaluation_interval: 200ms\nrule_files: [%q]\nscrape_configs:\n  - job_name: atlas\n    metrics_path: /metrics\n    authorization:\n      credentials: %q\n    static_configs:\n      - targets: [%q]\n", rules, runtime.Options.Token, strings.TrimPrefix(endpoint.URL, "http://"))
	if err = os.WriteFile(filepath.Join(dir, "prometheus.yaml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	log, err := os.Create(filepath.Join(dir, "prometheus.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(binary, "--config.file="+filepath.Join(dir, "prometheus.yaml"), "--web.listen-address="+address, "--storage.tsdb.path="+filepath.Join(dir, "tsdb"), "--storage.tsdb.retention.time=1h")
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	base := "http://" + address
	client := &http.Client{Timeout: time.Second}
	wait := func(check func() bool) {
		t.Helper()
		until := time.Now().Add(8 * time.Second)
		for time.Now().Before(until) && ctx.Err() == nil {
			if check() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("real Prometheus observation did not converge")
	}
	wait(func() bool {
		response, err := client.Get(base + "/-/ready")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == 200
	})
	alerts := func(name string) bool {
		response, err := client.Get(base + "/api/v1/alerts")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var out struct {
			Data struct {
				Alerts []struct {
					State  string            `json:"state"`
					Labels map[string]string `json:"labels"`
				} `json:"alerts"`
			} `json:"data"`
		}
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&out) != nil {
			return false
		}
		for _, alert := range out.Data.Alerts {
			if alert.Labels["alertname"] == name && alert.State == "firing" {
				return true
			}
		}
		return false
	}
	id := tenant(t)
	receive(t, id, "PROM-AUDIT", "seed", 10)
	if _, err = pool.Exec(ctx, `UPDATE inventory.outbox SET created_at=clock_timestamp()-interval '3 seconds' WHERE tenant_id=$1 AND sku='PROM-AUDIT'`, id); err != nil {
		t.Fatal(err)
	}
	audit := telemetry.Audit{Runtime: runtime, Pool: pool}
	if err = audit.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return alerts("AtlasConsumerLag") })
	if alerts("AtlasIntegrityFailure") {
		t.Fatal("lag fired the corruption alert")
	}
	for range 100 {
		worked, err := store.ProcessOne(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			break
		}
	}
	if err = audit.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return !alerts("AtlasConsumerLag") })
	if _, err = pool.Exec(ctx, `UPDATE inventory.stock_views SET on_hand=9 WHERE tenant_id=$1 AND sku='PROM-AUDIT'`, id); err != nil {
		t.Fatal(err)
	}
	if err = audit.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return alerts("AtlasIntegrityFailure") })
	if _, err = pool.Exec(ctx, `UPDATE inventory.stock_views SET on_hand=10 WHERE tenant_id=$1 AND sku='PROM-AUDIT'`, id); err != nil {
		t.Fatal(err)
	}
	if err = audit.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return !alerts("AtlasIntegrityFailure") })
	app := runtime.HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	failed := runtime.HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	for range 4 {
		app.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/catalog/products/PRIVATE", nil))
	}
	time.Sleep(500 * time.Millisecond)
	for range 3 {
		failed.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/catalog/products/PRIVATE", nil))
	}
	for range 3 {
		app.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/catalog/products/PRIVATE", nil))
	}
	query := func(expression string) (float64, bool) {
		response, err := client.Get(base + "/api/v1/query?query=" + url.QueryEscape(expression))
		if err != nil {
			return 0, false
		}
		defer response.Body.Close()
		var out struct {
			Data struct {
				Result []struct {
					Value []json.RawMessage `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if json.NewDecoder(response.Body).Decode(&out) != nil || len(out.Data.Result) != 1 || len(out.Data.Result[0].Value) != 2 {
			return 0, false
		}
		var text string
		if json.Unmarshal(out.Data.Result[0].Value[1], &text) != nil {
			return 0, false
		}
		v, err := strconv.ParseFloat(text, 64)
		return v, err == nil && !math.IsNaN(v) && !math.IsInf(v, 0)
	}
	var availability, latency float64
	wait(func() bool {
		value, ok := query("atlas:http_availability:ratio5m")
		availability = value
		return ok && value > 0.35 && value < 0.65
	})
	wait(func() bool {
		value, ok := query("atlas:http_latency:p95_5m")
		latency = value
		return ok && value > 0 && value <= 10
	})
	saveOperationsReport(t, "prometheus", map[string]any{"runtime": "3.2.1", "actual_lag_alert_fired": true, "lag_did_not_fire_integrity": true, "actual_integrity_alert_fired": true, "both_cleared_after_fixture_repair": true, "availability_ratio_observed": availability, "histogram_p95_estimate_seconds": latency, "signal_source": "actual PostgreSQL source/view audit, authenticated HTTP scrape, native PromQL rule evaluation"})
}
