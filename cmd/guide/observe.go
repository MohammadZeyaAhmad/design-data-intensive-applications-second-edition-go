package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/envelope"
)

func verifyObserveLive(c *client) error {
	u, _ := url.Parse(c.base)
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" {
		return errors.New("observability drill requires the local development profile")
	}
	token := os.Getenv("ATLAS_TELEMETRY_TOKEN")
	if len(token) < 32 {
		return errors.New("private telemetry reader credential is required")
	}
	started, item, err := startCheckout(c, "test-ok")
	if err != nil {
		return err
	}
	final, err := waitCheckout(c, started.ID)
	if err != nil || final.Status != "confirmed" {
		return errors.New("observed checkout did not confirm")
	}
	if !envelope.ValidTraceParent(final.Metadata.TraceParent) {
		return errors.New("profile did not persist a trace parent")
	}
	traceID := final.Metadata.TraceParent[3:35]
	httpClient := &http.Client{Timeout: 2 * time.Second}
	for _, port := range []int{19181, 19182, 19183} {
		address := fmt.Sprintf("http://127.0.0.1:%d/metrics", port)
		for _, credential := range []string{"", c.keys["acme"], token} {
			req, _ := http.NewRequest("GET", address, nil)
			if credential != "" {
				req.Header.Set("Authorization", "Bearer "+credential)
			}
			response, err := httpClient.Do(req)
			if err != nil {
				return errors.New("private metrics endpoint is unavailable")
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			response.Body.Close()
			if err != nil {
				return err
			}
			expected := 401
			if credential == token {
				expected = 200
			}
			if response.StatusCode != expected {
				return fmt.Errorf("metrics credential boundary returned %d", response.StatusCode)
			}
			for _, secret := range []string{token, c.keys["acme"], item, started.ID} {
				if secret != "" && bytes.Contains(body, []byte(secret)) {
					return errors.New("sensitive material reached scraped telemetry")
				}
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var traceSpans int
	var services map[string]bool
	var names map[string]int
	err = waitRPCReady(ctx, func() (bool, error) {
		response, err := httpClient.Get("http://127.0.0.1:19686/api/traces/" + traceID)
		if err != nil {
			return false, nil
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		if err != nil {
			return false, err
		}
		for _, secret := range []string{token, c.keys["acme"], item, started.ID} {
			if secret != "" && bytes.Contains(body, []byte(secret)) {
				return false, errors.New("sensitive material reached the trace backend")
			}
		}
		var out struct {
			Data []struct {
				Spans []struct {
					OperationName string `json:"operationName"`
				} `json:"spans"`
				Processes map[string]struct {
					ServiceName string `json:"serviceName"`
				} `json:"processes"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &out) != nil || len(out.Data) == 0 {
			return false, nil
		}
		names = map[string]int{}
		services = map[string]bool{}
		for _, span := range out.Data[0].Spans {
			names[span.OperationName]++
		}
		for _, process := range out.Data[0].Processes {
			services[process.ServiceName] = true
		}
		traceSpans = len(out.Data[0].Spans)
		for _, name := range []string{"http.request", "ordering.checkout.publish", "checkout.step", "payment_provider.request", "sql.query"} {
			if names[name] == 0 {
				return false, nil
			}
		}
		return services["atlas-api"] && services["atlas-worker"] && services["atlas-provider"], nil
	})
	if err != nil {
		return err
	}
	for _, address := range []string{"http://127.0.0.1:19290/-/ready", "http://127.0.0.1:19300/api/health", "http://127.0.0.1:19300/api/dashboards/uid/atlas-operations"} {
		if err = waitRPCReady(ctx, func() (bool, error) {
			response, err := httpClient.Get(address)
			if err != nil {
				return false, nil
			}
			defer response.Body.Close()
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			return response.StatusCode == 200, nil
		}); err != nil {
			return errors.New("a provisioned observation service did not become ready")
		}
	}
	fmt.Println("Independent metrics credential, provisioned dashboard and native Jaeger checkout trace verified.")
	return report("operations-live", map[string]any{"confirmed": true, "private_metrics_endpoints": 3, "tenant_credential_rejected_for_metrics": true, "jaeger_trace_id": traceID, "jaeger_spans": traceSpans, "services": services, "span_names": names, "provisioned_dashboard": "atlas-operations", "payload_privacy_checked": true})
}
