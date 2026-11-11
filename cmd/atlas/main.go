package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	graphqlhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/graphql"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/messaging"
	cathttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/httpapi"
	catdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/adapters/postgres"
	checkhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/httpapi"
	checkdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/postgres"
	checktemporal "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/adapters/temporal"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/advance"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/features/start"
	checkrun "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/checkout/runtime"
	fulfillhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/adapters/httpapi"
	fulfilldb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/adapters/postgres"
	fulfilldispatch "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/features/dispatch"
	fulfillrun "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/fulfillment/runtime"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/httpapi"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/adapters/postgres"
	ordershttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/httpapi"
	orderspostgres "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/ordering/adapters/postgres"
	payhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/httpapi"
	paydb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/payments/adapters/provider"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/config"
	inboxdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/messaging/postgres"
	searchhttp "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/adapters/httpapi"
	searchdb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/adapters/postgres"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("atlas stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	if len(os.Args) != 2 {
		return errors.New("usage: atlas serve|worker|provider|migrate|check|search-vector-init|broker-init|broker-publisher|broker-consumer|broker-status|temporal-worker|temporal-dispatcher|temporal-check")
	}
	command := os.Args[1]
	if command != "serve" && command != "worker" && command != "migrate" && command != "check" && command != "search-vector-init" && command != "provider" && command != "broker-init" && command != "broker-publisher" && command != "broker-consumer" && command != "broker-status" && command != "temporal-worker" && command != "temporal-dispatcher" && command != "temporal-check" {
		return errors.New("unknown atlas command")
	}
	if err := config.LoadLocalEnv(".env"); err != nil {
		return err
	}
	dsn := os.Getenv("ATLAS_DATABASE_URL")
	if dsn == "" {
		return errors.New("ATLAS_DATABASE_URL is required")
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	poolConfig.MaxConns = 16
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return errors.New("could not initialize database pool")
	}
	defer pool.Close()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err = pool.Ping(startup); err != nil {
		return errors.New("database is unavailable")
	}
	if command == "migrate" {
		if err = migrations.Apply(startup, pool); err != nil {
			return err
		}
		logger.Info("migrations applied")
		return nil
	}
	if err = migrations.Check(startup, pool); err != nil {
		return err
	}
	if command == "check" {
		logger.Info("database and schema ready")
		return nil
	}
	if command == "search-vector-init" {
		if err = searchdb.EnsureVector(startup, pool); err != nil {
			return err
		}
		return searchdb.CheckVector(startup, pool)
	}

	if strings.HasPrefix(command, "broker-") {
		return brokerCommand(ctx, logger, pool, command)
	}
	if command == "provider" {
		token := os.Getenv("ATLAS_PROVIDER_TOKEN")
		if len(token) < 32 {
			return errors.New("ATLAS_PROVIDER_TOKEN is required")
		}
		address := os.Getenv("ATLAS_HTTP_ADDR")
		if address == "" {
			address = "127.0.0.1:18082"
		}
		return serveHTTP(ctx, logger, address, provider.Server{Pool: pool, Token: token}.Handler())
	}
	providerURL := os.Getenv("ATLAS_PROVIDER_URL")
	if providerURL == "" {
		providerURL = "http://127.0.0.1:18082"
	}
	providerClient, err := provider.NewClient(providerURL, os.Getenv("ATLAS_PROVIDER_TOKEN"))
	if err != nil {
		return err
	}
	catalogStore := &catdb.Store{Pool: pool}
	paymentStore := &paydb.Service{Pool: pool, Provider: providerClient}
	checkoutStore := &checkdb.Store{Pool: pool}
	store := &postgres.Store{Pool: pool}
	orderStore := &orderspostgres.Store{Pool: pool}
	engine := os.Getenv("ATLAS_CHECKOUT_ENGINE")
	if engine == "" {
		engine = "postgres"
	}
	if engine != "postgres" && engine != "temporal" {
		return errors.New("unknown checkout engine")
	}
	orderCheckout := orderspostgres.Checkout{Store: orderStore, Engine: engine}
	checkoutWorker := checkrun.Worker{Store: checkoutStore, Orders: orderStore, Handler: advance.Handler{Inventory: postgres.Workflow{Store: store}, Payments: paymentStore, Orders: orderCheckout}}
	if strings.HasPrefix(command, "temporal-") {
		return temporalCommand(ctx, logger, checkoutStore, checktemporal.Activities{Store: checkoutStore, Handler: checkoutWorker.Handler}, command)
	}
	fulfillmentStore := &fulfilldb.Store{Pool: pool}
	fulfillmentWorker := fulfillrun.Worker{Store: fulfillmentStore, Orders: orderStore, Handler: fulfilldispatch.Handler{Inventory: postgres.Workflow{Store: store}}}
	transport := os.Getenv("ATLAS_MESSAGE_TRANSPORT")
	if transport == "" {
		transport = "postgres"
	}
	if transport != "postgres" && transport != "kafka" {
		return errors.New("unknown message transport")
	}
	if transport == "kafka" {
		checkoutWorker.Orders = nil
		fulfillmentWorker.Orders = nil
	}
	if command == "worker" {
		logger.Info("Atlas inventory, ordering, checkout and fulfillment workers started")
		group, workerContext := errgroup.WithContext(ctx)
		group.Go(func() error {
			if transport == "kafka" {
				return store.RunExpiry(workerContext, func(err error) { logger.Error("inventory expiry retry", "error", err) })
			}
			return store.Run(workerContext, func(err error) { logger.Error("inventory worker attempt failed", "error", err) })
		})
		group.Go(func() error {
			if transport == "kafka" {
				return (inboxdb.Inbox{Pool: pool, Group: api.Group}).Run(workerContext, brokerHandler(store, orderStore, checkoutStore, fulfillmentStore), func(error) { logger.Error("inbox application retry") })
			}
			return orderStore.Run(workerContext, func(err error) { logger.Error("ordering projection attempt failed", "error", err) })
		})
		group.Go(func() error {
			return checkoutWorker.Run(workerContext, func(err error) { logger.Error("checkout step retry", "error", err) })
		})
		group.Go(func() error {
			return fulfillmentWorker.Run(workerContext, func(err error) { logger.Error("fulfillment retry", "error", err) })
		})
		return group.Wait()
	}
	keys, err := config.TenantKeys(os.Getenv("ATLAS_TENANT_KEYS"))
	if err != nil {
		return err
	}
	address := os.Getenv("ATLAS_HTTP_ADDR")
	if address == "" {
		address = "127.0.0.1:8080"
	}
	mux := http.NewServeMux()
	graphqlHTTP, err := graphqlhttp.New(graphqlhttp.Options{Orders: orderStore, Catalog: catalogStore, TenantKeys: keys, CursorKey: []byte(os.Getenv("ATLAS_GRAPHQL_CURSOR_KEY"))})
	if err != nil {
		return err
	}
	mux.Handle("/v1/graphql", graphqlHTTP)
	orderHTTP := ordershttp.New(ordershttp.Options{Store: orderStore, TenantKeys: keys, Logger: logger})
	mux.Handle("/v1/orders", orderHTTP)
	mux.Handle("/v1/orders/", orderHTTP)
	mux.Handle("/", httpapi.New(httpapi.Options{Store: store, TenantKeys: keys, Ready: func(ctx context.Context) error { return migrations.Check(ctx, pool) }, Logger: logger}))

	checkoutHTTP := checkhttp.New(start.Handler{Catalog: catalogStore, Orders: orderCheckout}, checkoutStore, keys)
	mux.Handle("POST /v1/orders/{id}/checkout", checkoutHTTP)
	mux.Handle("/v1/checkouts/", checkoutHTTP)
	mux.Handle("/v1/catalog/", cathttp.New(catalogStore, keys))
	vectorFlag := os.Getenv("ATLAS_SEARCH_VECTOR_ENABLED")
	if vectorFlag != "" && vectorFlag != "false" && vectorFlag != "true" {
		return errors.New("ATLAS_SEARCH_VECTOR_ENABLED must be true or false")
	}
	searchStore := &searchdb.Store{Pool: pool, VectorEnabled: vectorFlag == "true"}
	if searchStore.VectorEnabled {
		if err = searchdb.CheckVector(startup, pool); err != nil {
			return err
		}
	}
	mux.Handle("/v1/search/", searchhttp.New(searchStore, keys))
	fulfillmentHTTP := fulfillhttp.New(fulfillmentStore, keys)
	mux.Handle("/v1/shipments/", fulfillmentHTTP)
	mux.Handle("GET /v1/orders/{id}/fulfillment", fulfillmentHTTP)
	mux.Handle("/v1/payments/", payhttp.New(paymentStore, pool, keys))
	return serveHTTP(ctx, logger, address, mux)
}
func serveHTTP(ctx context.Context, logger *slog.Logger, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	logger.Info("Atlas API started", "address", address, "authentication", "development tenant keys")
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
