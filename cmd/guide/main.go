package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "guide:", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: guide configure|doctor|wait|demo|demo-orders|demo-checkout|verify-checkout|verify-checkout-recovery|verify-fulfillment|verify-fulfillment-recovery|verify-operations|verify-kafka|verify-temporal|verify-storage|verify-btree|verify-indexes|verify-columnstore|verify-graph|verify-graph-services|verify-models|verify-models-live|verify-search|verify-search-vector|verify-search-live|verify-live|verify-lifecycle|verify-orders|verify-recovery|coverage")
	}
	command := os.Args[1]
	if command == "configure" {
		return configure()
	}
	if command == "verify-search-vector" {
		return verifySearchVector()
	}
	if command == "verify-graph-services" {
		return verifyGraphServices()
	}
	if command == "verify-graph" || command == "verify-search" || command == "verify-models" {
		cmd := exec.Command("make", command)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}
	if command == "coverage" {
		return coverage()
	}
	if command == "verify-storage" || command == "verify-btree" || command == "verify-indexes" || command == "verify-columnstore" {
		if err := coverage(); err != nil {
			return err
		}
		target, lab, output := "./ch04/...", "lsm", "../artifacts/storage.json"
		if command == "verify-btree" {
			target, lab, output = "./ch04/btree", "btree", "../artifacts/btree.json"
		}
		if command == "verify-columnstore" {
			target, lab, output = "./ch04/columnstore", "columnstore", "../artifacts/columnstore.json"
		}
		steps := [][]string{{"test", "-race", "-count=1", target}, {"run", "./cmd/lab", lab, "-output", output}}
		if command == "verify-indexes" {
			steps = [][]string{{"test", "-race", "-count=1", "./ch04/indexes", "./ch04/storage"}, {"run", "./cmd/lab", "indexes", "-output", "../artifacts/indexes.json"}, {"run", "./cmd/lab", "tradeoffs", "-output", "../artifacts/tradeoffs.json"}}
		}
		for _, args := range steps {
			cmd := exec.Command("go", args...)
			cmd.Dir = "labs"
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return err
			}
		}
		if command == "verify-indexes" {
			cmd := exec.Command("go", "run", "./benchmarks/storage/cmd/pgplan", "-output", "artifacts/query-plans.json")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return cmd.Run()
		}
		return nil
	}
	if command == "doctor" {
		for _, name := range []string{"go", "git", "docker"} {
			if _, err := exec.LookPath(name); err != nil {
				return fmt.Errorf("missing tool: %s", name)
			}
		}
		fmt.Println("Go, Git, and Docker are available. Run make up to validate services.")
		return nil
	}
	if err := config.LoadLocalEnv(".env"); err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	if err = waitReady(c); err != nil {
		return err
	}
	switch command {
	case "verify-models-live":
		return verifyModelsLive(c)
	case "verify-search-live":
		return verifySearchLive(c)
	case "wait":
		fmt.Println("API and database schema are ready.")
		return nil
	case "demo":
		return demo(c)
	case "demo-checkout":
		return demoCheckout(c)
	case "verify-checkout-recovery":
		return verifyCheckoutRecovery(c)
	case "verify-kafka":
		return verifyKafka(c)
	case "verify-temporal":
		return verifyTemporal(c)
	case "verify-operations":
		return verifyOperations(c)
	case "verify-fulfillment":
		return verifyFulfillment(c, false)
	case "verify-fulfillment-recovery":
		return verifyFulfillment(c, true)
	case "verify-checkout":
		return verifyCheckout(c)
	case "demo-orders":
		return demoOrders(c)
	case "verify-lifecycle":
		return verifyLifecycle(c)
	case "verify-orders":
		return verifyOrders(c)
	case "verify-live":
		return verify(c)
	case "verify-recovery":
		return recovery(c)
	default:
		return errors.New("unknown guide command")
	}
}

func secret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
func configure() error {
	f, err := os.OpenFile(".env", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		fmt.Println("Existing credentials preserved; new local settings added when needed.")
		return providerConfig()
	}
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			_ = os.Remove(".env")
		}
	}()
	password, err := secret()
	if err != nil {
		return err
	}
	alpha, err := secret()
	if err != nil {
		return err
	}
	beta, err := secret()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "POSTGRES_PASSWORD=%s\nATLAS_DATABASE_URL=postgresql://atlas:%s@127.0.0.1:15433/atlas_dev?sslmode=disable\nATLAS_TEST_DATABASE_URL=postgresql://atlas:%s@127.0.0.1:15433/atlas_test?sslmode=disable\nATLAS_TENANT_KEYS=acme=%s,globex=%s\nATLAS_HTTP_ADDR=127.0.0.1:18081\nATLAS_BASE_URL=http://127.0.0.1:18081\n", password, password, password, alpha, beta)
	if err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	ok = true
	fmt.Println("Created .env with random local database and tenant credentials (mode 0600). Values were not printed.")
	return providerConfig()
}

type client struct {
	correlation string
	base        string
	keys        map[string]string
	http        *http.Client
}

func waitReady(c *client) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, _, err := c.request("GET", "/readyz", "", "", nil)
		if err == nil && status == http.StatusOK {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("API did not become ready within 30 seconds")
}

func newClient() (*client, error) {
	if _, err := config.TenantKeys(os.Getenv("ATLAS_TENANT_KEYS")); err != nil {
		return nil, err
	}
	base := os.Getenv("ATLAS_BASE_URL")
	if base == "" {
		base = "http://127.0.0.1:18081"
	}
	u, err := url.Parse(base)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("invalid ATLAS_BASE_URL")
	}
	keys := map[string]string{}
	for _, entry := range strings.Split(os.Getenv("ATLAS_TENANT_KEYS"), ",") {
		tenant, key, _ := strings.Cut(entry, "=")
		keys[tenant] = key
	}
	if keys["acme"] == "" || keys["globex"] == "" {
		return nil, errors.New("guide scenarios require development tenants acme and globex")
	}
	return &client{base: strings.TrimRight(base, "/"), keys: keys, http: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("redirects are not allowed for authenticated guide requests")
	}}}, nil
}
func (c *client) request(method, path, tenant, key string, value any) (int, []byte, error) {
	var body io.Reader
	if value != nil {
		b, err := json.Marshal(value)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return 0, nil, err
	}
	if tenant != "" {
		req.Header.Set("Authorization", "Bearer "+c.keys[tenant])
	}
	if c.correlation != "" {
		req.Header.Set("X-Correlation-ID", c.correlation)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if value != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	r, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return r.StatusCode, b, err
}
func (c *client) expect(method, path, tenant, key string, value any, status int) ([]byte, error) {
	code, b, err := c.request(method, path, tenant, key, value)
	if err != nil {
		return nil, err
	}
	if code != status {
		return nil, fmt.Errorf("%s %s: expected %d, got %d: %s", method, path, status, code, b)
	}
	return b, nil
}
func sku(prefix string) (string, error) { id, err := identity.NewID(); return prefix + id, err }

func demo(c *client) error {
	sku, err := sku("demo-")
	if err != nil {
		return err
	}
	if _, err = c.expect("POST", "/v1/stock/"+sku+"/receipts", "acme", sku+"-receipt", map[string]int{"quantity": 10}, 200); err != nil {
		return err
	}
	b, err := c.expect("POST", "/v1/reservations", "acme", sku+"-reserve", map[string]any{"sku": sku, "quantity": 2}, 201)
	if err != nil {
		return err
	}
	var out ports.Outcome
	if err = json.Unmarshal(b, &out); err != nil {
		return err
	}
	b, err = c.expect("POST", "/v1/reservations/"+out.Reservation.ID+"/commit", "acme", sku+"-commit", nil, 200)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func verify(c *client) error {
	sku, err := sku("race-")
	if err != nil {
		return err
	}
	if _, err = c.expect("POST", "/v1/stock/"+sku+"/receipts", "acme", sku+"-receipt", map[string]int{"quantity": 10}, 200); err != nil {
		return err
	}
	if _, err = c.expect("GET", "/v1/stock/"+sku, "globex", "", nil, 404); err != nil {
		return err
	}
	if _, err = c.expect("GET", "/v1/stock/"+sku, "", "", nil, 401); err != nil {
		return err
	}
	var success, denied atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, err := c.request("POST", "/v1/reservations", "acme", fmt.Sprintf("%s-%d", sku, i), map[string]any{"sku": sku, "quantity": 1})
			if err != nil {
				errs <- err
				return
			}
			switch status {
			case 201:
				success.Add(1)
			case 409:
				denied.Add(1)
			default:
				errs <- fmt.Errorf("unexpected reservation status %d", status)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	if success.Load() != 10 || denied.Load() != 90 {
		return fmt.Errorf("oversell check failed: %d succeeded, %d denied", success.Load(), denied.Load())
	}
	b, err := c.expect("GET", "/v1/stock/"+sku, "acme", "", nil, 200)
	if err != nil {
		return err
	}
	var stock domain.StockState
	if err = json.Unmarshal(b, &stock); err != nil {
		return err
	}
	if stock.OnHand != 10 || stock.Reserved != 10 || stock.Allocated != 0 || stock.Version != 11 {
		return errors.New("unexpected authoritative stock after contention")
	}
	if err = waitProjection(c, sku, stock); err != nil {
		return err
	}
	return report("live", map[string]any{"passed": true, "concurrent_requests": 100, "accepted": success.Load(), "rejected": denied.Load(), "tenant_isolation": true, "projection_matches_authority": true})
}

func waitProjection(c *client, sku string, want domain.StockState) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		status, b, err := c.request("GET", "/v1/stock/"+sku+"/projection", "acme", "", nil)
		if err != nil {
			return err
		}
		if status == 200 {
			var got domain.StockState
			if err = json.Unmarshal(b, &got); err != nil {
				return err
			}
			if got == want {
				return nil
			}
		} else if status != 404 {
			return fmt.Errorf("projection returned %d", status)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("projection did not catch up within 15 seconds")
}

func compose(args ...string) error {
	base := []string{"compose", "--env-file", ".env", "-f", "deploy/compose/compose.yaml"}
	if os.Getenv("ATLAS_MESSAGE_TRANSPORT") == "kafka" {
		base = append(base, "-f", "deploy/compose/kafka.yaml")
	}
	if os.Getenv("ATLAS_CHECKOUT_ENGINE") == "temporal" {
		base = append(base, "-f", "deploy/compose/temporal.yaml")
	}
	base = append(base, "-p", "ddia-go")
	cmd := exec.Command("docker", append(base, args...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
func recovery(c *client) (result error) {
	u, _ := url.Parse(c.base)
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" {
		return errors.New("recovery checks only run against the local development stack")
	}
	if err := compose("stop", "worker"); err != nil {
		return err
	}
	restarted := false
	defer func() {
		if !restarted {
			result = errors.Join(result, compose("start", "worker"))
		}
	}()
	sku, err := sku("recovery-")
	if err != nil {
		return err
	}
	if _, err = c.expect("POST", "/v1/stock/"+sku+"/receipts", "acme", sku+"-receipt", map[string]int{"quantity": 7}, 200); err != nil {
		return err
	}
	command := map[string]any{"sku": sku, "quantity": 3}
	b, err := c.expect("POST", "/v1/reservations", "acme", sku+"-reserve", command, 201)
	if err != nil {
		return err
	}
	var out ports.Outcome
	if err = json.Unmarshal(b, &out); err != nil {
		return err
	}
	if _, err = c.expect("GET", "/v1/stock/"+sku+"/projection", "acme", "", nil, 404); err != nil {
		return err
	}
	orderKey, err := identity.NewID()
	if err != nil {
		return err
	}
	orderBody, err := c.expect("POST", "/v1/orders", "acme", orderKey, orderCommand("BOOK", 2), 201)
	if err != nil {
		return err
	}
	orderState, err := decodeOrder(orderBody)
	if err != nil {
		return err
	}
	if _, err = c.expect("GET", "/v1/orders/"+orderState.ID+"/projection", "acme", "", nil, 404); err != nil {
		return err
	}
	if err = compose("start", "worker"); err != nil {
		return err
	}
	restarted = true
	if err = waitProjection(c, sku, out.Stock); err != nil {
		return err
	}
	if err = waitOrder(c, orderState); err != nil {
		return err
	}
	if err = compose("restart", "api"); err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		status, _, err := c.request("GET", "/readyz", "", "", nil)
		if err == nil && status == 200 {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		return errors.New("API did not become ready after restart")
	}
	replayed, err := c.expect("POST", "/v1/reservations", "acme", sku+"-reserve", command, 201)
	if err != nil {
		return err
	}
	if !bytes.Equal(b, replayed) {
		return errors.New("idempotent response changed after API restart")
	}
	orderReplay, err := c.expect("POST", "/v1/orders", "acme", orderKey, orderCommand("BOOK", 2), 201)
	if err != nil {
		return err
	}
	if !bytes.Equal(orderBody, orderReplay) {
		return errors.New("order response changed after API restart")
	}
	return report("recovery", map[string]any{"passed": true, "worker_outage_backlog_recovered": true, "projection_matches_authority": true, "order_history_and_projection_recovered": true, "same_response_after_api_restart": true})
}

func report(name string, data any) error {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll("artifacts", 0755); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join("artifacts", name+".json"), append(b, '\n'), 0644); err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
func coverage() error {
	files := map[string]int{"book-section-coverage.csv": 476, "glossary-coverage.csv": 61, "book-index-traceability.csv": 4215, "backend-pattern-catalog.csv": 48}
	for name, want := range files {
		f, err := os.Open(filepath.Join("docs", "plan", name))
		if err != nil {
			return err
		}
		rows, err := csv.NewReader(f).ReadAll()
		f.Close()
		if err != nil {
			return err
		}
		if len(rows)-1 != want {
			return fmt.Errorf("coverage inventory changed: %s has %d rows, expected %d; review scope before updating the baseline", name, len(rows)-1, want)
		}
		fmt.Printf("%s: %d tracked entries\n", name, want)
	}
	fmt.Println("This validates source traceability, not implementation completion. See docs/STATUS.md.")
	return implementationProgress()
}
