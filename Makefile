SHELL := /bin/bash
.NOTPARALLEL:
.PHONY: configure doctor build up down test integration verify verify-live verify-lifecycle verify-orders verify-recovery demo demo-orders demo-checkout verify-checkout verify-checkout-recovery verify-fulfillment verify-fulfillment-recovery verify-operations coverage-check lab fmt
COMPOSE = docker compose --env-file .env -f deploy/compose/compose.yaml -p ddia-go

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
verify: test integration coverage-check lab verify-live verify-lifecycle verify-orders verify-checkout verify-checkout-recovery verify-fulfillment verify-fulfillment-recovery verify-operations verify-recovery
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
	gofmt -w cmd contracts internal migrations tests labs

verify-fulfillment:
	go run ./cmd/guide verify-fulfillment
verify-fulfillment-recovery:
	go run ./cmd/guide verify-fulfillment-recovery

verify-operations:
	go run ./cmd/guide verify-operations
