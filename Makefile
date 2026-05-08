.PHONY: run watcher snapshot build clean \
        db-up db-down db-reset \
        test test-unit test-integration test-race \
        bench bench-cpu bench-mem \
        profile profile-cpu profile-mem \
        cover tidy \
        proto proto-tools

# =============================================================================
# Run targets — start one of the three executables locally.
# =============================================================================

# Run the API server
run:
	go run ./cmd/api

# Run the event watcher
watcher:
	go run ./cmd/event-watcher

# Run the snapshot runner
snapshot:
	go run ./cmd/snapshot-runner

# Build all binaries
build:
	go build -o bin/portfolio-api ./cmd/api
	go build -o bin/event-watcher ./cmd/event-watcher
	go build -o bin/snapshot-runner ./cmd/snapshot-runner
	go build -o bin/event-persister ./cmd/event-persister
	go build -o bin/event-router ./cmd/event-router
	go build -o bin/event-notifier ./cmd/event-notifier
	go build -o bin/event-analytics ./cmd/event-analytics

# Remove build artifacts
clean:
	rm -rf bin/
	rm -f cpu.out mem.out *.test

# =============================================================================
# Database helpers.
# =============================================================================

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3
	@echo "Database reset complete. Migrations ran automatically."

# =============================================================================
# Module 4 Aula 2 — Brokers (Kafka + RabbitMQ).
# =============================================================================

broker-up:
	docker compose up -d kafka rabbitmq
	docker compose run --rm kafka-init
	@echo "Kafka:    localhost:9092 (topic: wallet.events.v1, 3 partitions)"
	@echo "RabbitMQ: localhost:5672 (mgmt UI: http://localhost:15672, guest/guest)"

broker-down:
	docker compose stop kafka rabbitmq

broker-logs:
	docker compose logs -f kafka rabbitmq

# Bring up everything the chapter needs (DB + brokers).
infra-up:
	docker compose up -d postgres kafka rabbitmq
	docker compose run --rm kafka-init

infra-down:
	docker compose down

# =============================================================================
# Tests.
#
# The repository has three layers of tests:
#
#   1. Unit tests          — always run, no Docker required.
#                            Target: `make test-unit`
#
#   2. Race tests          — same tests, under -race. Best used in CI.
#                            Target: `make test-race`
#
#   3. Integration tests   — require Docker; spin up Postgres + Anvil via
#                            testcontainers. Gated by the `integration` tag.
#                            Target: `make test-integration`
#
# The default `make test` runs unit + integration so developers who have
# Docker running get full coverage locally. CI can pick granular targets.
# =============================================================================

test: test-unit test-integration

test-unit:
	go test ./... -count=1 -timeout 2m

test-integration:
	go test -tags=integration ./... -count=1 -timeout 10m

test-race:
	go test ./... -count=1 -race -timeout 3m

# =============================================================================
# Benchmarks.
#
# `-run=^$` skips regular tests so only benchmarks execute. `-benchmem`
# reports allocations, which is what you usually want when optimising.
# =============================================================================

bench:
	go test ./... -bench=. -benchmem -run=^$$ -benchtime=1s

bench-cpu:
	go test ./internal/watcher -bench=BenchmarkNormalizeTransferLog -benchmem -run=^$$ \
	    -cpuprofile=cpu.out -benchtime=2s
	@echo "CPU profile written to cpu.out — explore with: go tool pprof cpu.out"

bench-mem:
	go test ./internal/snapshot -bench=BenchmarkRunner_Run_50Wallets -benchmem -run=^$$ \
	    -memprofile=mem.out -benchtime=2s
	@echo "Memory profile written to mem.out — explore with: go tool pprof mem.out"

# =============================================================================
# Profiling convenience wrappers.
# =============================================================================

profile: profile-cpu

profile-cpu:
	@echo "Generating CPU profile over the watcher normalizer hot path..."
	go test ./internal/watcher -bench=BenchmarkNormalizeTransferLog -run=^$$ \
	    -benchtime=3s -cpuprofile=cpu.out
	@echo ""
	@echo "Interactive exploration:  go tool pprof -http=:6060 cpu.out"
	@echo "Top 10 hottest functions:  go tool pprof -top -nodecount=10 cpu.out"

profile-mem:
	@echo "Generating allocation profile over the snapshot runner..."
	go test ./internal/snapshot -bench=BenchmarkRunner_Run_50Wallets -run=^$$ \
	    -benchtime=3s -memprofile=mem.out
	@echo ""
	@echo "Interactive exploration:  go tool pprof -http=:6060 mem.out"

# =============================================================================
# Coverage + housekeeping.
# =============================================================================

cover:
	go test ./... -coverprofile=coverage.out -count=1
	go tool cover -func=coverage.out | tail -20

tidy:
	go mod tidy

# =============================================================================
# Protobuf / gRPC codegen.
#
# Module 4 introduces a gRPC mirror of the REST API. Sources live in
# proto/, generated Go lands in gen/, and we commit the output so users
# can build the project without installing buf.
#
# `make proto-tools` installs the codegen toolchain into $GOPATH/bin.
# `make proto` regenerates Go from the .proto sources. Run it whenever
# you edit anything under proto/.
# =============================================================================

proto-tools:
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

proto:
	@command -v buf >/dev/null 2>&1 || { echo "buf not on PATH — run 'make proto-tools' first"; exit 1; }
	buf lint
	buf generate
	@echo "Generated code regenerated under gen/"
