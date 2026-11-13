// loadgen is deliberately finite: worker, request, body, sample and time bounds
// are validated before traffic starts. It never retries a mutation.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type observation struct {
	status  int
	cache   string
	latency time.Duration
	err     error
}
type report struct {
	Requests    int            `json:"requests"`
	Completed   int            `json:"completed"`
	Concurrency int            `json:"concurrency"`
	Peak        int64          `json:"peak_inflight"`
	Status      map[int]int    `json:"http_statuses"`
	Cache       map[string]int `json:"cache_results"`
	Failures    int            `json:"failures"`
	P50         float64        `json:"p50_ms"`
	P95         float64        `json:"p95_ms"`
	Max         float64        `json:"max_ms"`
	Duration    float64        `json:"duration_ms"`
	SKU         string         `json:"sku"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}
func run() error {
	if err := config.LoadLocalEnv(".env"); err != nil {
		return err
	}
	defaultBase := os.Getenv("ATLAS_BASE_URL")
	if defaultBase == "" {
		defaultBase = "http://127.0.0.1:18081"
	}
	flags := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	base := flags.String("base", defaultBase, "local Atlas base URL")
	tenant := flags.String("tenant", "acme", "configured development tenant")
	sku := flags.String("sku", "", "existing SKU; omitted only with -seed")
	seed := flags.Bool("seed", false, "create one synthetic product with a fresh command identity")
	requests := flags.Int("requests", 96, "finite read count (1..1000)")
	concurrency := flags.Int("concurrency", 16, "worker count (1..64)")
	timeout := flags.Int("timeout-ms", 1500, "per-request timeout (10..5000 ms)")
	output := flags.String("output", "artifacts/resilience-load.json", "JSON report")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *requests < 1 || *requests > 1000 || *concurrency < 1 || *concurrency > 64 || *concurrency > *requests || *timeout < 10 || *timeout > 5000 || (*sku == "" && !*seed) || (*sku != "" && *seed) {
		return errors.New("invalid bounded load configuration; provide -sku or -seed")
	}
	parsed, err := url.Parse(*base)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") {
		return errors.New("load generator requires a loopback Atlas URL")
	}
	if _, err = config.TenantKeys(os.Getenv("ATLAS_TENANT_KEYS")); err != nil {
		return err
	}
	token := ""
	for _, entry := range strings.Split(os.Getenv("ATLAS_TENANT_KEYS"), ",") {
		name, key, _ := strings.Cut(entry, "=")
		if name == *tenant {
			token = key
		}
	}
	if token == "" {
		return errors.New("tenant is not configured")
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, MaxConnsPerHost: *concurrency, MaxIdleConnsPerHost: *concurrency, MaxIdleConns: *concurrency, ResponseHeaderTimeout: time.Duration(*timeout) * time.Millisecond}, Timeout: time.Duration(*timeout) * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}
	defer client.CloseIdleConnections()
	root := strings.TrimRight(*base, "/")
	if *seed {
		id, err := identity.NewID()
		if err != nil {
			return err
		}
		*sku = "load-" + id
		b, _ := json.Marshal(map[string]any{"name": "Load fixture", "currency": "USD", "unit_price_minor": 1200, "expected_version": 0})
		r, _ := http.NewRequestWithContext(ctx, "PUT", root+"/v1/catalog/products/"+*sku, bytes.NewReader(b))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "load-seed-"+id)
		response, err := client.Do(r)
		if err != nil {
			return errors.New("seed request failed")
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		response.Body.Close()
		if response.StatusCode != 200 {
			return fmt.Errorf("seed returned status %d", response.StatusCode)
		}
	}
	// PathEscape rejects path injection while preserving the actual SKU check.
	target := root + "/v1/catalog/products/" + url.PathEscape(*sku) + "?consistency=cached&min_version=1"
	jobs := make(chan struct{})
	results := make(chan observation, *concurrency)
	var workers sync.WaitGroup
	var active, peak atomic.Int64
	started := time.Now()
	for range *concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range jobs {
				n := active.Add(1)
				for previous := peak.Load(); n > previous; previous = peak.Load() {
					if peak.CompareAndSwap(previous, n) {
						break
					}
				}
				began := time.Now()
				r, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
				r.Header.Set("Authorization", "Bearer "+token)
				response, err := client.Do(r)
				result := observation{err: err}
				if err == nil {
					result.status = response.StatusCode
					result.cache = response.Header.Get("X-Cache")
					if result.status == 200 {
						var p domain.Product
						decoder := json.NewDecoder(io.LimitReader(response.Body, 4096))
						if decoder.Decode(&p) != nil || decoder.Decode(new(any)) != io.EOF || p.SKU != *sku || p.Version < 1 || !domain.ValidProduct(p) {
							result.err = errors.New("invalid product response")
						}
					}
					_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
					response.Body.Close()
					if result.status != 200 && result.status != 429 && result.status != 503 {
						result.err = errors.New("unexpected HTTP status")
					}
				}
				result.latency = time.Since(began)
				active.Add(-1)
				results <- result
			}
		}()
	}
	go func() {
		defer close(jobs)
		for range *requests {
			select {
			case jobs <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { workers.Wait(); close(results) }()
	summary := report{Requests: *requests, Concurrency: *concurrency, Status: map[int]int{}, Cache: map[string]int{}, SKU: *sku}
	samples := make([]float64, 0, *requests)
	for result := range results {
		summary.Completed++
		summary.Status[result.status]++
		if result.cache != "" {
			summary.Cache[result.cache]++
		}
		if result.err != nil {
			summary.Failures++
		}
		samples = append(samples, float64(result.latency)/float64(time.Millisecond))
	}
	summary.Peak = peak.Load()
	summary.Duration = float64(time.Since(started)) / float64(time.Millisecond)
	sort.Float64s(samples)
	if len(samples) > 0 {
		summary.P50 = samples[(len(samples)-1)/2]
		summary.P95 = samples[(len(samples)-1)*95/100]
		summary.Max = samples[len(samples)-1]
	}
	body, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if *output != "" {
		if err = os.MkdirAll(filepath.Dir(*output), 0755); err != nil {
			return err
		}
		if err = os.WriteFile(*output, body, 0600); err != nil {
			return err
		}
	}
	if _, err = os.Stdout.Write(body); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if summary.Failures > 0 || summary.Completed != *requests || summary.Status[200] == 0 {
		return errors.New("bounded load reported failed or incomplete requests")
	}
	return nil
}
