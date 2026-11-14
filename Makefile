SHELL := /bin/bash
.NOTPARALLEL:
.PHONY: configure doctor build up down test integration verify verify-live verify-lifecycle verify-orders verify-recovery demo demo-orders demo-checkout verify-checkout verify-checkout-recovery verify-fulfillment verify-fulfillment-recovery verify-operations coverage-check lab fmt
COMPOSE_KAFKA_TEST = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/kafka.yaml -p ddia-go-broker-test
COMPOSE_KAFKA = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/kafka.yaml -p ddia-go
COMPOSE = docker compose --env-file .env -f deploy/compose/compose.yaml -p ddia-go
COMPOSE_TEMPORAL = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/temporal.yaml -p ddia-go
COMPOSE_TEMPORAL_TEST = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/temporal.yaml -p ddia-go-temporal-test
COMPOSE_SEARCH = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/search-vector.yaml -p ddia-go-search
COMPOSE_SEARCH_TEST = docker compose --env-file .env -f deploy/compose/search-vector-test.yaml -p ddia-go-search-test
COMPOSE_GRAPH = docker compose --env-file .env -f deploy/compose/graph/compose.yaml -p ddia-go-graph
COMPOSE_ACTORS = docker compose -f deploy/compose/actors/compose.yaml -p ddia-go-actors-test
COMPOSE_ROUTING = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/routing/compose.yaml -p ddia-go
COMPOSE_OBSERVE = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/observe.yaml -p ddia-go
COMPOSE_RESILIENCE = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/resilience.yaml -p ddia-go
COMPOSE_REDIS_TEST = docker compose -f deploy/compose/resilience/redis-test.yaml -p ddia-go-cache-test
COMPOSE_RDF = docker compose --env-file .env -f deploy/compose/rdf/compose.yaml -p ddia-go-rdf

configure:
	go run ./cmd/guide configure
doctor:
	go run ./cmd/guide doctor
build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o bin/atlas ./cmd/atlas
up: configure build
	$(COMPOSE) up --build -d
	go run ./cmd/guide wait
down:
	$(COMPOSE) down
test:
	go test -race ./...
	cd labs && go test -race ./...
	go vet ./...
integration:
	go test -race -tags=integration -count=1 ./tests/integration
verify-live:
	go run ./cmd/guide verify-live
verify-lifecycle:
	go run ./cmd/guide verify-lifecycle
verify-orders:
	go run ./cmd/guide verify-orders
verify-recovery:
	go run ./cmd/guide verify-recovery
ifeq ($(strip $(ID)),)
verify: test integration coverage-check session-demo log-formats-demo object-storage-compare replication-model replication-demo resilience-test resilience-postgres cache-demo integration-redis lab models-demo models-postgres graph-demo integration-graph semantic-demo integration-semantic graphql-demo graphql-postgres check-encoding-generated encoding-demo check-avro-generated avro-demo evolution-demo check-rpc-generated rpc-demo actors-postgres actors-demo integration-actors storage-demo btree-demo index-demo tradeoffs-demo column-demo search-demo query-plans integration-search-vector verify-live verify-lifecycle verify-orders verify-checkout verify-checkout-recovery verify-fulfillment verify-fulfillment-recovery verify-operations verify-recovery verify-search-live verify-models-live verify-graphql-live
else ifeq ($(ID),OPS-01)
verify: verify-operations-baseline
else ifeq ($(ID),C06-01)
verify: verify-replication
else ifeq ($(ID),C06-02)
verify: verify-object-storage
else ifeq ($(ID),C06-03)
verify: verify-log-formats
else ifeq ($(ID),C06-04)
verify: verify-session-consistency
else ifeq ($(ID),APP-09)
verify: verify-resilience
else ifeq ($(ID),C03-01)
verify: verify-models
else ifeq ($(ID),C03-03)
verify: verify-graph
else ifeq ($(ID),C03-04)
verify: verify-semantic
else ifeq ($(ID),C03-05)
verify: verify-graphql
else ifeq ($(ID),C04-01)
verify: verify-storage
else ifeq ($(ID),C04-02)
verify: verify-btree
else ifeq ($(ID),C04-03)
verify: verify-indexes
else ifeq ($(ID),C04-04)
verify: verify-columnstore
else ifeq ($(ID),C04-05)
verify: verify-search
else ifeq ($(ID),C05-01)
verify: verify-encoding
else ifeq ($(ID),C05-02)
verify: verify-avro
else ifeq ($(ID),C05-03)
verify: verify-evolution
else ifeq ($(ID),C05-04)
verify: verify-rpc
else ifeq ($(ID),C05-06)
verify: verify-actors
else
verify:
	@echo 'Scoped verification supports ID=OPS-01, APP-09, C03-01, C03-03 through C03-05, C04-01 through C04-05, C05-01 through C05-04, C05-06, C06-01, C06-02, C06-03, C06-04; other plan IDs remain future interfaces.' >&2
	@exit 2
endif
demo:
	go run ./cmd/guide demo
demo-orders:
	go run ./cmd/guide demo-orders
demo-checkout:
	go run ./cmd/guide demo-checkout
verify-checkout:
	go run ./cmd/guide verify-checkout
verify-checkout-recovery:
	go run ./cmd/guide verify-checkout-recovery
coverage-check:
	go run ./cmd/guide coverage
lab:
	cd labs && go run ./cmd/lab lost-update
fmt:
	gofmt -w cmd contracts internal migrations tests api labs benchmarks

verify-fulfillment:
	go run ./cmd/guide verify-fulfillment
verify-fulfillment-recovery:
	go run ./cmd/guide verify-fulfillment-recovery

verify-operations:
	go run ./cmd/guide verify-operations

.PHONY: up-kafka down-kafka verify-kafka
up-kafka: configure build
	$(COMPOSE_KAFKA) up --build -d
	ATLAS_MESSAGE_TRANSPORT=kafka go run ./cmd/guide wait
down-kafka:
	$(COMPOSE_KAFKA) down
verify-kafka:
	ATLAS_MESSAGE_TRANSPORT=kafka go run ./cmd/guide verify-kafka

.PHONY: integration-kafka down-kafka-test
integration-kafka: configure
	ATLAS_KAFKA_PORT=19093 $(COMPOSE_KAFKA_TEST) up -d --wait kafka
	ATLAS_KAFKA_TEST_BROKERS=127.0.0.1:19093 go test -race -tags=integration,kafka -run '^TestKafka(Real|Consumer)' -count=1 ./tests/integration
	ATLAS_KAFKA_PORT=19093 $(COMPOSE_KAFKA_TEST) down
down-kafka-test:
	ATLAS_KAFKA_PORT=19093 $(COMPOSE_KAFKA_TEST) down

.PHONY: up-temporal down-temporal integration-temporal down-temporal-test verify-temporal
up-temporal: configure build
	$(COMPOSE_TEMPORAL) up --build -d --wait
	ATLAS_CHECKOUT_ENGINE=temporal go run ./cmd/guide wait
down-temporal:
	$(COMPOSE_TEMPORAL) down
integration-temporal: configure
	ATLAS_TEMPORAL_PORT=17233 ATLAS_TEMPORAL_UI_PORT=18233 $(COMPOSE_TEMPORAL_TEST) up -d --wait temporal
	ATLAS_TEMPORAL_TEST_ADDRESS=127.0.0.1:17233 go test -race -tags=integration,temporal -run '^TestTemporalReal' -count=1 ./tests/integration
	ATLAS_TEMPORAL_PORT=17233 ATLAS_TEMPORAL_UI_PORT=18233 $(COMPOSE_TEMPORAL_TEST) down
down-temporal-test:
	ATLAS_TEMPORAL_PORT=17233 ATLAS_TEMPORAL_UI_PORT=18233 $(COMPOSE_TEMPORAL_TEST) down
verify-temporal:
	ATLAS_CHECKOUT_ENGINE=temporal go run ./cmd/guide verify-temporal

.PHONY: storage-test storage-demo verify-storage
storage-test:
	cd labs && go test -race -count=1 ./ch04/...
storage-demo:
	cd labs && go run ./cmd/lab lsm -output ../artifacts/storage.json
verify-storage: coverage-check storage-test storage-demo

.PHONY: btree-test btree-demo verify-btree
btree-test:
	cd labs && go test -race -count=1 ./ch04/btree
btree-demo:
	cd labs && go run ./cmd/lab btree -output ../artifacts/btree.json
verify-btree: coverage-check btree-test btree-demo

.PHONY: index-test index-demo tradeoffs-demo query-plans storage-bench verify-indexes
index-test:
	cd labs && go test -race -count=1 ./ch04/indexes ./ch04/storage
index-demo:
	cd labs && go run ./cmd/lab indexes -output ../artifacts/indexes.json
tradeoffs-demo:
	cd labs && go run ./cmd/lab tradeoffs -output ../artifacts/tradeoffs.json
query-plans:
	go run ./benchmarks/storage/cmd/pgplan -output artifacts/query-plans.json
storage-bench:
	mkdir -p artifacts
	cd labs && ATLAS_BENCH_WORKDIR=../artifacts go test -run '^$$' -bench '^BenchmarkWarmPointRead$$' -benchtime=50ms -count=3 ./ch04/storage > ../artifacts/storage-go-benchmark.txt
verify-indexes: coverage-check index-test index-demo tradeoffs-demo query-plans storage-bench

.PHONY: column-test column-demo verify-columnstore
column-test:
	cd labs && go test -race -count=1 ./ch04/columnstore
column-demo:
	cd labs && go run ./cmd/lab columnstore -output ../artifacts/columnstore.json
verify-columnstore: coverage-check column-test column-demo

.PHONY: search-test search-demo verify-search verify-search-live up-search down-search integration-search-vector down-search-test
search-test:
	cd labs && go test -race -count=1 ./ch04/search
search-demo:
	cd labs && go run ./cmd/lab search -output ../artifacts/search.json
verify-search: coverage-check search-test search-demo integration-search-vector
verify-search-live:
	go run ./cmd/guide verify-search-live
up-search: configure build
	$(COMPOSE_SEARCH) up --build -d --wait
	go run ./cmd/guide wait
down-search:
	$(COMPOSE_SEARCH) down
integration-search-vector: configure
	@trap '$(COMPOSE_SEARCH_TEST) down' EXIT; $(COMPOSE_SEARCH_TEST) up -d --wait && go run ./cmd/guide verify-search-vector
down-search-test:
	$(COMPOSE_SEARCH_TEST) down

.PHONY: models-test models-demo models-postgres verify-models verify-models-live generate-catalog check-catalog-generated
models-test:
	cd labs && go test -race -count=1 ./ch03/models
models-demo:
	cd labs && go run ./cmd/lab models -output ../artifacts/models.json
models-postgres:
	ATLAS_MODELS_REPORT="$(CURDIR)/artifacts/models-postgres.json" go test -race -tags=integration -run '^TestCatalogModels' -count=1 ./tests/integration
verify-models: coverage-check models-test models-demo models-postgres
verify-models-live:
	go run ./cmd/guide verify-models-live
generate-catalog:
	bash tools/sqlc-catalog.sh generate
check-catalog-generated:
	bash tools/sqlc-catalog.sh check

.PHONY: graph-test graph-demo integration-graph verify-graph up-graph down-graph
graph-test:
	cd labs && go test -race -count=1 ./ch03/property-graph/...
graph-demo:
	cd labs && go run ./cmd/lab graph -output ../artifacts/graph.json
integration-graph: configure
	@trap '$(COMPOSE_GRAPH) down' EXIT; $(COMPOSE_GRAPH) up -d --wait && go run ./cmd/guide verify-graph-services
verify-graph: coverage-check graph-test graph-demo integration-graph
up-graph: configure
	$(COMPOSE_GRAPH) up -d --wait
down-graph:
	$(COMPOSE_GRAPH) down

.PHONY: prepare-rdf-runtime semantic-test semantic-demo integration-semantic verify-semantic up-rdf down-rdf
prepare-rdf-runtime:
	bash tools/prepare-rdf-runtime.sh
semantic-test:
	cd labs && go test -race -count=1 ./ch03/semantic-graph/...
semantic-demo:
	cd labs && go run ./cmd/lab semantic -output ../artifacts/semantic.json
integration-semantic: configure prepare-rdf-runtime
	@trap 'semantic_status=$$?; if (( semantic_status != 0 )); then $(COMPOSE_RDF) logs --no-color --tail 100; fi; $(COMPOSE_RDF) down; exit $$semantic_status' EXIT; $(COMPOSE_RDF) up --build -d --wait && go run ./cmd/guide verify-semantic-services
verify-semantic: coverage-check semantic-test semantic-demo integration-semantic
up-rdf: configure prepare-rdf-runtime
	$(COMPOSE_RDF) up --build -d --wait
down-rdf:
	$(COMPOSE_RDF) down

.PHONY: graphql-test graphql-demo graphql-postgres verify-graphql verify-graphql-live
graphql-test:
	go test -race -count=1 ./api/graphql ./tests/architecture
graphql-demo:
	go run ./benchmarks/graphql/cmd -output artifacts/graphql.json
graphql-postgres:
	ATLAS_GRAPHQL_REPORT="$(CURDIR)/artifacts/graphql-postgres.json" go test -race -tags=integration -run '^TestGraphQL' -count=1 ./tests/integration
verify-graphql: coverage-check graphql-test graphql-demo graphql-postgres
verify-graphql-live:
	go run ./cmd/guide verify-graphql-live

.PHONY: encoding-test encoding-demo encoding-bench generate-encoding check-encoding-generated verify-encoding
encoding-test:
	cd labs && go test -race -count=1 ./ch05/encoding ./cmd/gob-decode
encoding-demo:
	cd labs && go run ./cmd/lab encoding -output ../artifacts/encoding.json
encoding-bench:
	mkdir -p artifacts
	cd labs && go test -run '^$$' -bench '^BenchmarkRoundTrip$$' -benchtime=50ms -count=3 ./ch05/encoding > ../artifacts/encoding-go-benchmark.txt
generate-encoding:
	bash tools/encoding-generated.sh generate
check-encoding-generated:
	bash tools/encoding-generated.sh check
verify-encoding: coverage-check check-encoding-generated encoding-test encoding-demo encoding-bench

.PHONY: avro-test avro-demo generate-avro check-avro-generated verify-avro
avro-test:
	cd labs && go test -race -count=1 ./ch05/avro ./internal/avroprofile ./cmd/avro-gen
avro-demo:
	cd labs && go run ./cmd/lab avro -output ../artifacts/avro.json
generate-avro:
	bash tools/avro-generated.sh generate
check-avro-generated:
	bash tools/avro-generated.sh check
verify-avro: coverage-check check-avro-generated avro-test avro-demo

.PHONY: evolution-binaries evolution-test evolution-demo verify-evolution
evolution-binaries:
	bash tools/evolution-binaries.sh
evolution-test:
	go test -race -count=1 ./contracts/catalog ./internal/catalog/... ./tests/architecture
	go test -race -tags=integration -count=1 -run '^TestCatalogEvolution' ./tests/integration
evolution-demo: evolution-binaries
	go run ./cmd/guide evolution-demo
verify-evolution: coverage-check check-catalog-generated evolution-test evolution-demo

.PHONY: generate-rpc check-rpc-generated rpc-binaries rpc-test rpc-demo verify-rpc
generate-rpc:
	bash tools/rpc-generated.sh generate
check-rpc-generated:
	bash tools/rpc-generated.sh check
rpc-binaries:
	bash tools/rpc-binaries.sh
rpc-test:
	go test -race -count=1 ./api/grpc/... ./internal/inventory/... ./internal/platform/auth ./internal/platform/httpx ./internal/platform/routing ./tests/architecture
	cd labs && go test -race -count=1 ./ch05/rpc/...
	go test -race -tags=integration -count=1 -run '^TestRPC' ./tests/integration
rpc-demo: rpc-binaries
	go run ./cmd/guide rpc-demo
verify-rpc: coverage-check check-rpc-generated rpc-test rpc-demo

.PHONY: routing-build up-routing down-routing verify-rpc-live
routing-build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o bin/router ./cmd/router
up-routing: configure build routing-build
	chmod 0755 deploy/compose/routing/registry
	chmod 0644 deploy/compose/routing/registry/registry.json
	$(COMPOSE_ROUTING) up --build -d --wait rpc-left rpc-right router
down-routing:
	$(COMPOSE_ROUTING) rm --stop --force router rpc-left rpc-right
verify-rpc-live:
	go run ./cmd/guide verify-rpc-live

.PHONY: actors-binaries actors-test actors-demo actors-postgres actors-broker-demo integration-actors verify-actors up-actors down-actors
actors-binaries:
	bash tools/actors-binaries.sh
actors-test:
	cd labs && go test -race -count=1 ./ch05/actors/... ./internal/childprocess ./internal/disposabledatabase
actors-demo: actors-binaries
	go run ./cmd/guide actors-demo
actors-postgres:
	go run ./cmd/guide actors-postgres
actors-broker-demo: actors-binaries
	go run ./cmd/guide actors-broker-demo
integration-actors: actors-binaries
	@if test -n "$${ATLAS_ACTORS_BROKERS:-}"; then go run ./cmd/guide actors-broker-demo; else trap '$(COMPOSE_ACTORS) down --volumes' EXIT; $(COMPOSE_ACTORS) up -d --wait && go run ./cmd/guide actors-broker-demo; fi
verify-actors: coverage-check check-actors-contracts actors-test actors-postgres actors-demo integration-actors
up-actors:
	$(COMPOSE_ACTORS) up -d --wait
down-actors:
	$(COMPOSE_ACTORS) down --volumes

.PHONY: replication-binaries replication-test replication-postgres replication-model replication-demo verify-replication
replication-binaries:
	bash tools/replication-binaries.sh
replication-test:
	go test -race -count=1 ./internal/platform/database
	cd labs && go test -race -count=1 ./ch06/single-leader ./cmd/replication-lab
replication-postgres:
	go test -race -tags=integration -run '^TestWriterFence' -count=1 ./tests/integration
replication-model: replication-binaries
	artifacts/replication-binaries/replication-lab --model-only --output artifacts/replication/model.json
replication-demo: replication-binaries
	bash tools/replication-demo.sh
verify-replication: coverage-check replication-test replication-postgres replication-model replication-demo

.PHONY: object-storage-binaries object-storage-runtime object-storage-test object-storage-demo integration-object-storage object-storage-compare object-storage-s3-cloud verify-object-storage
object-storage-binaries:
	bash tools/object-storage-binaries.sh
object-storage-runtime:
	bash tools/object-storage-runtime.sh
object-storage-test:
	cd labs && go test -race -count=1 ./ch06/object-storage ./cmd/object-lab
	cd labs && go vet ./ch06/object-storage ./cmd/object-lab
object-storage-demo: object-storage-binaries
	artifacts/object-storage-binaries/object-lab --profile file --output artifacts/object-storage/file.json
integration-object-storage: object-storage-binaries object-storage-runtime
	artifacts/object-storage-binaries/object-lab --profile emulator --python "$${ATLAS_OBJECT_RUNTIME:-$(CURDIR)/artifacts/object-storage-runtime}/bin/python" --moto-script "$(CURDIR)/tools/object-storage-moto.py" --output artifacts/object-storage/emulator.json
object-storage-s3-cloud: object-storage-binaries
	artifacts/object-storage-binaries/object-lab --profile cloud --output artifacts/object-storage/cloud.json
object-storage-compare: object-storage-demo integration-object-storage
	python3 tools/object-storage-compare.py
verify-object-storage: coverage-check object-storage-test object-storage-compare

.PHONY: log-formats-binaries log-formats-test log-formats-demo verify-log-formats
log-formats-binaries:
	bash tools/log-formats-binaries.sh
log-formats-test:
	cd labs && go test -race -count=1 ./ch06/log-formats ./internal/pgprocess ./cmd/log-formats-lab
	cd labs && go vet ./ch06/log-formats ./internal/pgprocess ./cmd/log-formats-lab
log-formats-demo: log-formats-binaries
	bash tools/log-formats-demo.sh
verify-log-formats: coverage-check log-formats-test log-formats-demo

.PHONY: generate-actors-contracts check-actors-contracts
.PHONY: session-binaries session-test session-postgres session-demo verify-session-consistency
session-binaries:
	bash tools/session-binaries.sh
session-test:
	go test -race -count=1 ./internal/platform/readrouting ./internal/inventory/... ./tests/architecture
	cd labs && go test -race -count=1 ./ch06/session-consistency ./cmd/session-lab
	cd labs && go vet ./ch06/session-consistency ./cmd/session-lab
session-postgres:
	go test -race -tags=integration -run '^TestSession' -count=1 ./tests/integration
session-demo: session-binaries
	bash tools/session-demo.sh
verify-session-consistency: coverage-check session-test session-postgres session-demo

generate-actors-contracts:
	bash tools/actors-contracts.sh generate
check-actors-contracts:
	bash tools/actors-contracts.sh check

.PHONY: resilience-test resilience-postgres cache-demo integration-redis verify-resilience verify-resilience-live up-resilience down-resilience down-redis-test
resilience-test:
	go test -race -count=1 ./internal/platform/resilience ./internal/catalog/adapters/cache ./internal/payments/adapters/provider
	cd labs && go test -race -count=1 ./ch02/cache
resilience-postgres:
	ATLAS_RESILIENCE_REPORT="$(CURDIR)/artifacts/resilience-postgres.json" go test -race -tags=integration -run '^TestResilience' -count=1 ./tests/integration
cache-demo:
	cd labs && go run ./cmd/lab cache -output ../artifacts/cache-comparison.json
integration-redis: configure
	@trap '$(COMPOSE_REDIS_TEST) down --volumes' EXIT; $(COMPOSE_REDIS_TEST) up -d --wait && ATLAS_RESILIENCE_REDIS_REPORT="$(CURDIR)/artifacts/resilience-redis.json" go test -race -tags=integration,redis -run '^TestResilienceRedis' -count=1 ./tests/integration
verify-resilience: coverage-check resilience-test resilience-postgres cache-demo integration-redis
verify-resilience-live:
	go run ./cmd/guide verify-resilience-live
up-resilience: configure build
	$(COMPOSE_RESILIENCE) up --build -d
	go run ./cmd/guide wait
down-resilience:
	$(COMPOSE_RESILIENCE) down
down-redis-test:
	$(COMPOSE_REDIS_TEST) down --volumes

.PHONY: restore-default-cache
restore-default-cache:
	ATLAS_CACHE_BACKEND=memory ATLAS_CACHE_ADDRESS= $(COMPOSE) up -d api
	$(COMPOSE_RESILIENCE) stop redis
	$(COMPOSE_RESILIENCE) rm -f -v redis
	go run ./cmd/guide wait

.PHONY: observability-tools observability-test observability-postgres observability-prometheus observability-rules release-artifacts deployment-reference-check verify-operations-baseline up-observe down-observe restore-default-observe verify-observe-live
observability-tools:
	bash tools/operations-runtime.sh
observability-test:
	go test -race -count=1 ./internal/platform/telemetry/... ./contracts/envelope ./tests/architecture
observability-postgres:
	ATLAS_OPERATIONS_REPORT_DIR="$(CURDIR)/artifacts/operations" go test -race -tags=integration -run '^TestObservability' -count=1 ./tests/integration
observability-prometheus: observability-tools
	ATLAS_OPERATIONS_REPORT_DIR="$(CURDIR)/artifacts/operations" go test -race -tags=integration,observe -run '^TestObservabilityRealPrometheus' -count=1 ./tests/integration
observability-rules: observability-tools
	$${ATLAS_OPERATIONS_TOOLS:-$${TMPDIR:-/tmp}/atlas-operations-tools}/promtool check rules deploy/observe/rules.yaml
	cd deploy/observe && $${ATLAS_OPERATIONS_TOOLS:-$${TMPDIR:-/tmp}/atlas-operations-tools}/promtool test rules rules-test.yaml
release-artifacts:
	bash tools/release-artifacts.sh
deployment-reference-check:
	bash tools/helm-runtime.sh
	bash tools/helm-reference-check.sh
	bash tools/terraform-runtime.sh
	$${ATLAS_OPERATIONS_TOOLS:-$${TMPDIR:-/tmp}/atlas-operations-tools}/terraform -chdir=deploy/terraform/aws-reference fmt -check
verify-operations-baseline: coverage-check observability-test observability-postgres observability-prometheus observability-rules release-artifacts deployment-reference-check
	ATLAS_TELEMETRY_ENABLED=false ATLAS_DURABLE_TRACE=false $(MAKE) evolution-demo
up-observe: configure build
	$(COMPOSE_OBSERVE) up --build -d
	go run ./cmd/guide wait
verify-observe-live:
	go run ./cmd/guide verify-observe-live
down-observe:
	$(COMPOSE_OBSERVE) down
restore-default-observe:
	$(COMPOSE) up -d api worker provider
	$(COMPOSE_OBSERVE) stop prometheus grafana jaeger
	$(COMPOSE_OBSERVE) rm -f -v prometheus grafana jaeger
	go run ./cmd/guide wait
