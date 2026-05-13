# portfolio-api

> [Read this in Portuguese / Leia em Português](README.pt-BR.md)

Advanced Go course companion project. A production-inspired crypto-portfolio
service used across the later modules of an advanced Golang course taught by
[Bruno Campos](https://github.com/brunocampos-ssa). The code is intentionally
didactic: it looks like real backend code you could ship, but every piece is
written to teach a specific concept.

---

## Course Structure

The full course spans five modules. Module 1 is delivered through two smaller,
focused repositories. Starting with **Module 2**, the same `portfolio-api`
project evolves module-by-module: each module adds a new layer of
capability without rewriting what came before. Every module closes with a
git tag so students can check out the exact state at the end of the class.

| Module | Topic | Hours | Where |
|:-----:|:------|:-----:|:------|
| 1 | **Go Deep** — memory management, pointers, `interface{}`, reflection, error wrapping, panic/recover | 4h | [`go-memory-lab`](https://github.com/brunocampos-ssa/go-memory-lab) + [`go-advanced-interfaces`](https://github.com/brunocampos-ssa/go-advanced-interfaces) |
| 2 | **Advanced Concurrency** — channels, select, worker pools, pipelines, fan-in/fan-out, `sync`, `atomic`, `context` | 5h | This repo — tag [`module-2`](../../releases/tag/module-2) |
| 3 | **Professional Testing** — `testing`/`testify`/`mock`, integration & concurrency tests, benchmarking & profiling | 3h | This repo — tag [`module-3-class2`](../../releases/tag/module-3-class2) |
| 4 — Aula 1 | **JWT + gRPC** — argon2id, access + refresh tokens with rotation & replay detection, gRPC mirror of the REST API, dual-transport auth | 3h | This repo — tag [`module-4-class1`](../../releases/tag/module-4-class1) |
| 4 — Aula 2 | **Messaging (Kafka + RabbitMQ)** — cross-process event bus, three consumer groups with opposing semantics (resume / bridge / replay-from-zero), routing-key fan-out | 3h | This repo — **current state** |
| 5 | **Architecture & Integrations** — hexagonal architecture, custom middleware, observability (Prometheus/OpenTelemetry/Grafana), cross-platform build & Docker | 4h | Upcoming — same repo |

Each tag (`module-2`, `module-3`, …) is an annotated, signed git tag pointing
at the exact commit that concludes the corresponding module.

---

## What the Project Does

The service simulates a real crypto-portfolio backend:

- A **REST API** exposes a user's portfolio aggregated across multiple
  blockchain wallets (Ethereum, Klever) and prices the holdings in USD.
- An **event watcher** monitors Ethereum ERC-20 `Transfer` events for
  wallets stored in Postgres and persists normalized events.
- A **snapshot runner** generates point-in-time balance reports (a
  simulated tax-report input), fetching native ETH plus selected
  ERC-20 balances for every tracked wallet in parallel.

The domain stays small on purpose — just enough to make the architectural
and concurrency concepts land.

### Binaries

| Command | Purpose |
|---------|---------|
| `cmd/api` | JSON REST API + gRPC server with recovery/logging/request-id/JWT |
| `cmd/event-watcher` | Ethereum event poller; publishes envelopes to Kafka |
| `cmd/event-persister` | Kafka consumer group `wallet-events-persister` → `wallet_events` table |
| `cmd/event-router` | Kafka → RabbitMQ bridge; routing key `<network>.<direction>.<token>` |
| `cmd/event-notifier` | RabbitMQ consumer with configurable binding pattern (alerts) |
| `cmd/event-analytics` | Kafka replay-from-zero consumer; in-memory aggregates per hour |
| `cmd/snapshot-runner` | Batch pipeline: `Generate → WorkerPool → fan-in → persist` |

---

## Getting Started

### Prerequisites

- Go 1.25 (see `go.mod`)
- Docker (for Postgres and, optionally, integration tests)

### Run locally

```bash
# 1. Start Postgres + Kafka + RabbitMQ (also creates the Kafka topic).
make infra-up                         # or `make db-up` alone for Module 2/3 work

# 2. Start the REST + gRPC API.
make run

# 3. In another terminal, run the event watcher (publishes to Kafka).
make watcher

# 4. Run any subset of the event consumers (one per terminal).
make event-persister
make event-router
make event-analytics
NOTIFIER_BINDING_KEY="*.incoming.*" make event-notifier

# Or run the snapshot generator (Module 2/3 path, unrelated to the broker).
make snapshot

# Useful broker observability:
make broker-groups                    # list Kafka consumer groups + offsets + lag
make broker-tail                      # tail wallet.events.v1 from t=0
```

Default REST endpoints (Module 4 Aula 1 added the `/auth/*` group and gates `/users/{id}/*` behind a JWT):

```
POST /auth/register                # public
POST /auth/login                   # public
POST /auth/refresh                 # public
POST /auth/logout                  # public
GET  /health                       # public
GET  /users/{id}/portfolio         # JWT, self only
GET  /users/{id}/wallets           # JWT, self only
POST /users/{id}/wallets           # JWT, self only
GET  /debug/panic                  # educational only — demonstrates recovery middleware
```

The same service surface is also exposed over **gRPC** on port `:50051`
(see [Module 4 — Aula 1](#module-4--aula-1-jwt--grpc) below). A token
issued by either transport works on both — there is one auth core,
two surfaces.

Environment variables are listed in `.env.example`.

---

## Testing

Starting with Module 3, the project ships with a full testing layer:

```bash
make test-unit          # unit tests, no Docker required
make test-race          # same suite under `go test -race`
make test-integration   # Postgres + Anvil via testcontainers (tag `integration`)
make bench              # all benchmarks
make profile-cpu        # generates cpu.out — explore with `go tool pprof`
```

Highlights:

- Unit tests using `testing` + `testify/require` / `assert` + table tests.
- Mock-based tests at external boundaries using `testify/mock`
  (`internal/testutil/mocks`).
- Race-oriented tests for worker pools, fan-out and pipelines.
- Integration tests with a single `testenv.Setup()` entry point driven
  by `TestMain`. It boots Postgres + a forked-mainnet Anvil
  (`test/infrastructure/anvil/Dockerfile`, embedded via `go:embed`),
  applies DB migrations, and then **bootstraps the forked chain from the
  seeded database**: every distinct ETH wallet in the DB is given a
  deterministic native balance plus whale-sourced ERC-20 balances defined
  in `internal/testutil/testenv/fixtures.json`. Tests then just exercise
  business logic — no container juggling, no tx helpers inline. Runs by
  default against `https://eth.drpc.org/`; override with
  `TEST_ETH_FORK_URL=<your-rpc>` if you hit rate limits.
- Benchmarks + a `make profile-cpu` / `make profile-mem` workflow for
  teaching `pprof`.

Full teaching-grade documentation of the testing layer lives in the
Portuguese [`README.pt-BR.md`](README.pt-BR.md) (section "Módulo 3 — Testes
Profissionais"). It covers, in depth:

1. Testing pyramid strategy.
2. How the custom Anvil image works and why we wrap it with `socat`.
3. How to mutate chain state in tests (`anvil_setBalance`,
   `anvil_impersonateAccount`, `anvil_setStorageAt`).
4. Mocking patterns and when **not** to reach for a mock.
5. Running benchmarks and reading `pprof` output.

---

## Module 4 — Aula 1: JWT + gRPC

Module 4 Class 1 adds authentication and a second transport without
rewriting any of the Module 2/3 code. The story is "one auth core,
two transports": the same `auth.TokenVerifier` powers both the HTTP
middleware and the gRPC interceptor, and a token issued by either
transport works on both.

### What you get

- **argon2id password hashing** with PHC string format (OWASP-recommended).
  See `internal/auth/argon2.go`.
- **JWT access tokens** (HS256, 15-minute TTL, hard-pinned `iss=portfolio-api`).
  See `internal/auth/jwt.go`.
- **Refresh tokens** — opaque random 32 bytes, stored as SHA-256 hash,
  rotated on every use, with **replay detection that revokes the entire
  chain** when a previously-rotated token is presented again.
  See `internal/service/auth_service.go` and migration `005_add_user_auth.up.sql`.
- **REST endpoints** — `POST /auth/{register,login,refresh,logout}`.
- **JWT middleware** — `internal/middleware/jwt.go` validates the
  `Authorization: Bearer <token>` header and stashes claims onto request
  context.
- **gRPC mirror** — `proto/portfolio/v1/{auth,portfolio,wallet}.proto`
  with `buf` codegen, served on `:50051` alongside the HTTP server with
  graceful shutdown.
- **Self-only access** — protected handlers enforce
  `JWT subject == path id`. Cross-account requests get 403 / PermissionDenied.
- **Two example clients** — `examples/restclient` and `examples/grpcclient`
  that walk the full lifecycle end-to-end. Stdlib only.

### Trying it

```bash
# Generate gRPC code (only needed if you edit the .proto files).
make proto-tools     # one-time: install buf + plugins to $GOPATH/bin
make proto

# Run the API (boots HTTP on :8080 AND gRPC on :50051).
make run

# REST flow.
go run ./examples/restclient

# gRPC flow.
go run ./examples/grpcclient

# curl reference (the things the example client does):
curl -X POST localhost:8080/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","email":"alice@example.com","password":"correct-horse-battery"}'

curl -X POST localhost:8080/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"alice@example.com","password":"correct-horse-battery"}'

curl -H 'Authorization: Bearer <access_token>' \
  localhost:8080/users/<user_id>/portfolio

# grpcurl reference (server reflection is always-on in this teaching
# codebase — convenient for grpcurl/grpcui; production deployments would
# typically gate it behind a flag):
grpcurl -plaintext -d '{"email":"alice@example.com","password":"correct-horse-battery"}' \
  localhost:50051 portfolio.v1.AuthService/Login

grpcurl -plaintext \
  -H 'authorization: Bearer <access_token>' \
  -d '{"user_id":"<id>"}' \
  localhost:50051 portfolio.v1.PortfolioService/GetPortfolio
```

### Error mapping (REST ↔ gRPC)

| Domain code         | HTTP                  | gRPC                  |
|---------------------|-----------------------|-----------------------|
| `not_found`         | 404 Not Found         | NotFound              |
| `validation_error`  | 400 Bad Request       | InvalidArgument       |
| `invalid_input`     | 400 Bad Request       | InvalidArgument       |
| `unauthenticated`   | 401 Unauthorized      | Unauthenticated       |
| `forbidden`         | 403 Forbidden         | PermissionDenied      |
| `conflict`          | 409 Conflict          | AlreadyExists         |
| `provider_failure`  | 502 Bad Gateway       | Unavailable           |
| `upstream_timeout`  | 504 Gateway Timeout   | DeadlineExceeded      |
| `internal_error`    | 500                   | Internal              |

### What Aula 1 left open

Refresh and Logout are **REST-only** in Class 1 (the gRPC AuthService
exposes only Register and Login), and the auth layer still uses
`log.Printf` rather than structured logging. These items were the
original Aula 2 plan — see the retrospective note at the start of
the Aula 2 section below for the pivot.

Full teaching-grade walkthrough — argon2id internals, the two-token
model, the rotation-and-replay narrative, and side-by-side curl/grpcurl
demos — lives in the Portuguese [`README.pt-BR.md`](README.pt-BR.md)
(section "Módulo 4 — Aula 1").

---

## Module 4 — Aula 2: Messaging (Kafka + RabbitMQ)

Module 4 Class 2 takes the Module 2 in-process pipeline (watcher +
three channel-fed consumers in one binary) and distributes it across
**five processes communicating over two brokers**: Kafka as the
durable source of truth, and RabbitMQ as the topic-routed fan-out for
filtered delivery. The headline lesson is "same topic, three different
read patterns" — three Kafka consumer groups, three opposing semantics
(resume / bridge / replay-from-zero), all from one producer.

> **Pivot note:** Module 4 Aula 1's "What's deferred to Aula 2"
> originally promised auth observability + resilience + gRPC
> refresh/logout. Aula 2 ended up going in a different direction —
> messaging — so those auth-arc items remain on the wishlist for a
> future class.

### Topology

```
[poller] → [normalize] → [Kafka publisher] → wallet.events.v1
                                                    │
              ┌─────────────────────────────────────┼──────────────────────────────┐
              ▼                                     ▼                              ▼
       event-persister                       event-router                   event-analytics
       (CG: persister)                       (CG: router)                   (CG: …-UnixNano)
              │                                     │                              │
              ▼                                     ▼                              ▼
       wallet_events                       wallet.events                   in-memory map
       (Postgres)                          (RabbitMQ topic exchange)       (replay from t=0
                                                  │                          on every restart)
                                                  ▼
                                          event-notifier
                                          (queue bound by *.incoming.*)
```

### What you get

- **Envelope schema** — `broker.EventEnvelope` (JSON, snake_case) with
  `SchemaVersion` for forward-compatible evolution and a deterministic
  `EventID` derived from `(network, tx_hash, log_index, wallet_id,
  direction)`. `Marshal`/`Unmarshal` are symmetric and both run
  `Validate`. See `internal/broker/event.go`.
- **Broker abstraction** — `broker.Publisher`, `broker.Consumer`,
  `broker.Handler`. Business code never imports `kafka-go` or
  `amqp091-go`. See `internal/broker/publisher.go`.
- **Kafka adapter** — `RequiredAcks=All`, partition key = `WalletID`
  (per-wallet ordering preserved), group-aware consumer with bounded
  retry (3 attempts, exponential backoff), poison-pill skip, and a
  `NewReplayConsumer` constructor that the analytics binary uses for
  replay-from-zero semantics (unique-per-process group ID + skip
  commits). See `internal/broker/kafka/`.
- **RabbitMQ adapter** — topic exchange, `Persistent` delivery, manual
  ack/nack, publisher confirms. Routing key built by `broker.RoutingKey`
  as `<network>.<direction>.<token_lowercase>`. See
  `internal/broker/rabbitmq/`.
- **Three Kafka consumer groups, three read patterns:**
  - `event-persister` — resumes from committed offsets; idempotent on
    `EventID` as `wallet_events` PRIMARY KEY (`ON CONFLICT (id) DO NOTHING`).
  - `event-router` — bridges Kafka → RabbitMQ, stamping the EventID
    into AMQP `MessageId` and an `event_id` header for downstream
    dedupe.
  - `event-analytics` — **replays from t=0 on every restart**.
    Aggregates `(network, direction, token, hour) → {count, total}`
    in memory and dumps periodically to stdout. State is ephemeral by
    design; Kafka is the source of truth.
- **`event-notifier`** — RabbitMQ consumer; binding pattern set via
  `NOTIFIER_BINDING_KEY` (`*.incoming.*`, `*.*.usdc`, `ethereum.#`,
  …). Same binary serves every alerting use case.
- **`docker-compose.yml`** — Kafka in KRaft mode (no Zookeeper) with a
  **dual-listener config** so in-compose clients (kafka-init, kafka-tail)
  and host clients (the Go binaries, testcontainers) both reach the
  broker correctly. RabbitMQ ships with the management UI on
  `http://localhost:15672` (guest/guest).

### Trying it

```bash
# Bring up Postgres + Kafka + RabbitMQ + create the topic with 3 partitions.
make infra-up

# Producer.
make watcher

# Consumers (one per terminal).
make event-persister
make event-router
make event-analytics
NOTIFIER_BINDING_KEY="*.incoming.*" make event-notifier

# Visualize the three consumer groups at different offsets on the same
# topic — the "aha" moment of the lesson.
make broker-groups

# Tail the raw Kafka stream from t=0 (host-side; useful while watching
# the watcher publish).
make broker-tail
```

### Headline integration tests

| Test | What it proves |
|------|----------------|
| `TestEventPersister_FullPath_BlockchainToDB` | 5-hop end-to-end: Anvil ERC-20 transfer → watcher → Kafka → persister → row in `wallet_events` |
| `TestEventNotifier_FullPipeline_BlockchainToAlert` | The full 6-hop pipeline reaching the notifier callback |
| `TestEventAnalytics_RestartReplaysFromZero` | First run consumes N envelopes; restart with the same base group ID **also** consumes N — proves the replay semantics (would hang if offsets were committed) |
| `TestKafkaConsumer_GroupsHaveIndependentOffsets` | Two groupIDs read the same stream independently |
| `TestKafkaPublisher_PartitionAffinityByWalletID` | Messages with the same `WalletID` land on the same partition (ordering guarantee) |

Full teaching-grade walkthrough — broker comparison, partition-key
trade-offs, at-least-once with bounded retry, replay-from-zero recipe,
routing-key hierarchy, and a 10-step suggested classroom flow — lives
in the Portuguese [`README.pt-BR.md`](README.pt-BR.md) (section "Módulo
4 — Aula 2").

---

## Project Layout

```
portfolio-api/
├── cmd/
│   ├── api/                       # REST + gRPC server entrypoint (Module 4)
│   ├── event-watcher/             # Ethereum event monitor → Kafka (Module 4 Aula 2)
│   ├── event-persister/           # Kafka consumer → Postgres (Module 4 Aula 2)
│   ├── event-router/              # Kafka → RabbitMQ bridge (Module 4 Aula 2)
│   ├── event-notifier/            # RabbitMQ filtered alerts (Module 4 Aula 2)
│   ├── event-analytics/           # Kafka replay-from-zero aggregator (Module 4 Aula 2)
│   └── snapshot-runner/           # Wallet balance snapshot generator
├── internal/
│   ├── analytics/                 # In-memory event aggregator (Module 4 Aula 2)
│   ├── auth/                      # argon2id + JWT issuer/verifier (Module 4)
│   │   ├── argon2.go              # PHC-encoded argon2id hasher
│   │   └── jwt.go                 # HS256 issuer/verifier + claims context
│   ├── broker/                    # Cross-process event bus (Module 4 Aula 2)
│   │   ├── event.go               # EventEnvelope wire format + Validate
│   │   ├── publisher.go           # Publisher / Consumer / Handler interfaces
│   │   ├── topics.go              # Kafka topic + group IDs, exchange, RoutingKey
│   │   ├── kafka/                 # segmentio/kafka-go adapter (Publisher + Consumer + ReplayConsumer)
│   │   └── rabbitmq/              # rabbitmq/amqp091-go adapter (Publisher + Consumer)
│   ├── concurrent/                # Reusable concurrency helpers (Module 2)
│   ├── config/                    # Environment-driven configuration
│   ├── contracts/                 # Port interfaces (repositories & providers)
│   ├── domain/                    # Entities, sentinel errors, AppError
│   ├── grpcapi/                   # gRPC server + auth interceptor (Module 4)
│   ├── httpapi/                   # HTTP handlers & response helpers
│   ├── middleware/                # RequestID, Logging, Recovery, JWT (Module 4)
│   ├── notifier/                  # RabbitMQ notify Handler (Module 4 Aula 2)
│   ├── persister/                 # Kafka → Postgres Handler (Module 4 Aula 2)
│   ├── provider/
│   │   ├── blockchain/            # Ethereum / Klever adapters
│   │   └── pricing/               # CoinGecko + mock price provider
│   ├── repository/postgres/       # Repository implementations
│   │   └── refresh_token_repository.go  # (Module 4)
│   ├── router/                    # Kafka → RabbitMQ bridge Handler (Module 4 Aula 2)
│   ├── service/
│   │   ├── auth_service.go        # Register/Login/Refresh/Logout (Module 4)
│   │   └── portfolio_service.go
│   ├── snapshot/                  # Snapshot runner (pipeline)
│   ├── watcher/                   # Event watcher (poller → normalize → Kafka)
│   └── testutil/                  # Shared test infrastructure (Module 3 + 4)
├── proto/portfolio/v1/            # .proto sources (Module 4)
├── gen/portfolio/v1/              # generated Go code from buf (Module 4)
├── examples/                      # Runnable client demos (Module 4)
│   ├── restclient/                # End-to-end REST flow over stdlib http
│   └── grpcclient/                # End-to-end gRPC flow with bearer metadata
├── migrations/                    # SQL migrations (embedded via //go:embed)
├── test/
│   ├── infrastructure/anvil/      # Custom Anvil Dockerfile
│   └── integration/               # End-to-end tests (build tag: integration)
├── buf.yaml                       # buf module config (Module 4)
├── buf.gen.yaml                   # buf codegen config (Module 4)
├── docker-compose.yml             # Postgres + Kafka (KRaft) + RabbitMQ
├── Makefile
├── EXERCISES.md                   # Practical exercises by module
├── README.md                      # This file (English)
└── README.pt-BR.md                # Full class handout (Brazilian Portuguese)
```

---

## Exercises

Each module ships practical exercises in [`EXERCISES.md`](EXERCISES.md)
(currently Brazilian Portuguese). They are designed to be completed in the
order shown and build on the base code already in the repository.

---

## Tags & Releases

| Tag | Module | State |
|-----|--------|-------|
| [`module-2`](../../releases/tag/module-2) | 2 — Advanced Concurrency | Closed |
| [`module-3-class2`](../../releases/tag/module-3-class2) | 3 — Professional Testing | Closed |
| [`module-4-class1`](../../releases/tag/module-4-class1) | 4 — JWT + gRPC | Closed |
| `module-4-class2` (upcoming) | 4 — Messaging (Kafka + RabbitMQ) | Current work |
| `module-5` (planned) | 5 — Architecture & Integrations | — |

Checkout any module's state with `git checkout module-<N>`.

---

## Contributing

This is a class project and the primary audience is students enrolled in
the course. That said, improvements that make the teaching material
clearer (bug fixes, wording, typo corrections, additional exercises) are
welcome via pull request.

When contributing:

- Keep production code **minimally changed** — the modules build on each
  other, and rewrites break that continuity.
- New tests are always welcome. Follow the patterns already in place
  (table-driven + `testify/require`, mocks only at external boundaries).
- Run `make test-unit && make test-race` before opening a PR.

---

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).
