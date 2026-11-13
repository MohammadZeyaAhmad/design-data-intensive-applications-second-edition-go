package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	cat "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
)

func resilienceCompose(ctx context.Context, args ...string) error {
	base := []string{"compose", "--env-file", ".env", "-f", "deploy/compose/compose.yaml", "-f", "deploy/compose/resilience.yaml", "-p", "ddia-go"}
	cmd := exec.CommandContext(ctx, "docker", append(base, args...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
func apiIncarnation(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", "compose", "--env-file", ".env", "-f", "deploy/compose/compose.yaml", "-p", "ddia-go", "ps", "-q", "api")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return "", errors.New("local API container is required")
	}
	cmd = exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.Id}} {{.State.StartedAt}}", id)
	out, err = cmd.Output()
	return strings.TrimSpace(string(out)), err
}
func cachedDisplay(ctx context.Context, c *client, sku string, minVersion int64) (domain.Product, string, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/v1/catalog/products/%s?consistency=cached&min_version=%d", c.base, url.PathEscape(sku), minVersion), nil)
	if err != nil {
		return domain.Product{}, "", err
	}
	request.Header.Set("Authorization", "Bearer "+c.keys["acme"])
	response, err := c.http.Do(request)
	if err != nil {
		return domain.Product{}, "", err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("X-Consistency") != "bounded-stale" {
		return domain.Product{}, "", fmt.Errorf("cached read returned %d", response.StatusCode)
	}
	var product domain.Product
	if err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&product); err != nil {
		return product, "", err
	}
	return product, response.Header.Get("X-Cache"), nil
}
func verifyResilienceLive(c *client) (result error) {
	u, _ := url.Parse(c.base)
	if (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") || u.Port() != "18081" {
		return errors.New("outage drill requires the local ddia-go API on port 18081")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	before, err := apiIncarnation(ctx)
	if err != nil {
		return err
	}
	item, err := sku("cache-")
	if err != nil {
		return err
	}
	path := "/v1/catalog/products/" + item
	publish := func(version, price int64) error {
		_, err := c.expect("PUT", path, "acme", fmt.Sprintf("%s-v%d", item, version+1), map[string]any{"name": "Resilience fixture", "currency": "USD", "unit_price_minor": price, "expected_version": version}, 200)
		return err
	}
	if err = publish(0, 1200); err != nil {
		return err
	}
	if _, _, err = cachedDisplay(ctx, c, item, 0); err != nil {
		return err
	}
	_, warm, err := cachedDisplay(ctx, c, item, 0)
	if err != nil || warm != "hit" {
		return fmt.Errorf("cache did not warm: %s %w", warm, err)
	}
	if err = resilienceCompose(ctx, "stop", "redis"); err != nil {
		return err
	}
	stopped := true
	defer func() {
		if stopped {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := resilienceCompose(cleanup, "start", "redis"); err != nil {
				result = errors.Join(result, err)
			}
		}
	}()
	if err = publish(1, 2300); err != nil {
		return err
	}
	authoritative, err := c.expect("GET", path, "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var strong domain.Product
	if err = json.Unmarshal(authoritative, &strong); err != nil {
		return err
	}
	fallback, state, err := cachedDisplay(ctx, c, item, 2)
	if err != nil {
		return err
	}
	if state != "fallback" || fallback.Version != 2 || fallback.UnitPriceMinor != 2300 || strong != fallback {
		return fmt.Errorf("cache outage failed authoritative fallback: state=%s version=%d", state, fallback.Version)
	}
	rawQuote, err := c.expect("POST", "/v1/catalog/quotes", "acme", item+"-quote", map[string]any{"lines": []domain.Line{{SKU: item, Quantity: 2}}}, 201)
	if err != nil {
		return err
	}
	var frozen cat.Quote
	if err = json.Unmarshal(rawQuote, &frozen); err != nil {
		return err
	}
	if frozen.TotalMinor != 4600 || len(frozen.Lines) != 1 || frozen.Lines[0].PriceVersion != 2 {
		return errors.New("outage quote did not use the committed price")
	}
	if err = resilienceCompose(ctx, "start", "redis"); err != nil {
		return err
	}
	stopped = false
	recovered := ""
	if err = waitRPCReady(ctx, func() (bool, error) {
		p, status, err := cachedDisplay(ctx, c, item, 2)
		if err != nil {
			return false, err
		}
		recovered = status
		return status == "hit" && p.Version == 2, nil
	}); err != nil {
		return err
	}
	after, err := apiIncarnation(ctx)
	if err != nil {
		return err
	}
	if after != before {
		return errors.New("healthy API restarted during Redis recovery")
	}
	report := map[string]any{"warm": warm, "outage": state, "recovered": recovered, "authoritative_version": strong.Version, "quote_total_minor": frozen.TotalMinor, "quote_price_version": frozen.Lines[0].PriceVersion, "same_api_incarnation": before == after, "redis_persistence": "disabled; cache loss is expected", "boundaries": "notifications and version floors are advisory; quotes use PostgreSQL"}
	if err = os.MkdirAll("artifacts", 0755); err != nil {
		return err
	}
	body, _ := json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile("artifacts/resilience-live.json", append(body, '\n'), 0600); err != nil {
		return err
	}
	load := exec.CommandContext(ctx, "go", "run", "./cmd/loadgen", "-sku", item, "-requests", "128", "-concurrency", "32", "-output", "artifacts/resilience-load.json")
	load.Stdout = os.Stdout
	load.Stderr = os.Stderr
	if err = load.Run(); err != nil {
		return err
	}
	fmt.Println("Redis outage preserved Catalog/quote correctness; cache recovered with the same API process.")
	return nil
}
