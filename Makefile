SHELL := /bin/bash
.NOTPARALLEL:
.PHONY: configure doctor build up down test integration verify verify-live verify-lifecycle verify-orders verify-recovery demo demo-orders demo-checkout verify-checkout verify-checkout-recovery verify-fulfillment verify-fulfillment-recovery verify-operations coverage-check lab fmt
COMPOSE_KAFKA_TEST = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/kafka.yaml -p ddia-go-broker-test
COMPOSE_KAFKA = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/kafka.yaml -p ddia-go
COMPOSE = docker compose --env-file .env -f deploy/compose/compose.yaml -p ddia-go
COMPOSE_TEMPORAL = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/temporal.yaml -p ddia-go
COMPOSE_TEMPORAL_TEST = docker compose --env-file .env -f deploy/compose/compose.yaml -f deploy/compose/temporal.yaml -p ddia-go-temporal-test

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
verify: test integration coverage-check lab storage-demo btree-demo index-demo tradeoffs-demo column-demo query-plans verify-live verify-lifecycle verify-orders verify-checkout verify-checkout-recovery verify-fulfillment verify-fulfillment-recovery verify-operations verify-recovery
else ifeq ($(ID),C04-01)
verify: verify-storage
else ifeq ($(ID),C04-02)
verify: verify-btree
else ifeq ($(ID),C04-03)
verify: verify-indexes
else ifeq ($(ID),C04-04)
verify: verify-columnstore
else
verify:
	@echo 'Scoped verification currently supports ID=C04-01 through C04-04; other plan IDs remain future interfaces.' >&2
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
	gofmt -w cmd contracts internal migrations tests labs benchmarks

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
