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
| 4 — Aula 1 | **JWT + gRPC** — argon2id, access + refresh tokens with rotation & replay detection, gRPC mirror of the REST API, dual-transport auth | 3h | This repo — **current state** |
| 4 — Aula 2 | **Auth observability & resilience** — structured logging, audit, rate-limit, gRPC tracing/metrics interceptors | 3h | Upcoming — same repo |
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
| `cmd/api` | JSON REST API with recovery/logging/request-id middleware |
| `cmd/event-watcher` | Long-running process: `poller → Stage → FanOut → consumers` |
| `cmd/snapshot-runner` | Batch pipeline: `Generate → WorkerPool → fan-in → persist` |

---

## Getting Started

### Prerequisites

- Go 1.25 (see `go.mod`)
- Docker (for Postgres and, optionally, integration tests)

### Run locally

```bash
# 1. Start Postgres (migrations run automatically).
make db-up

# 2. Start the REST API.
make run

# 3. In another terminal, run the event watcher.
make watcher

# 4. Or run the snapshot generator.
make snapshot
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

# grpcurl reference (server reflection is enabled in dev):
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

### What's deferred to Aula 2

Refresh and Logout are **REST-only** in Class 1 (the gRPC AuthService
exposes only Register and Login). Class 2 will round out the gRPC
surface alongside structured logging (`slog`), an auth audit log, login
rate limiting, and gRPC observability interceptors.

Full teaching-grade walkthrough — argon2id internals, the two-token
model, the rotation-and-replay narrative, and side-by-side curl/grpcurl
demos — lives in the Portuguese [`README.pt-BR.md`](README.pt-BR.md)
(section "Módulo 4 — Aula 1").

---

## Project Layout

```
portfolio-api/
├── cmd/
│   ├── api/                       # REST + gRPC server entrypoint (Module 4)
│   ├── event-watcher/             # Ethereum event monitor
│   └── snapshot-runner/           # Wallet balance snapshot generator
├── internal/
│   ├── auth/                      # argon2id + JWT issuer/verifier (Module 4)
│   │   ├── argon2.go              # PHC-encoded argon2id hasher
│   │   └── jwt.go                 # HS256 issuer/verifier + claims context
│   ├── concurrent/                # Reusable concurrency helpers (Module 2)
│   ├── config/                    # Environment-driven configuration
│   ├── contracts/                 # Port interfaces (repositories & providers)
│   ├── domain/                    # Entities, sentinel errors, AppError
│   ├── grpcapi/                   # gRPC server + auth interceptor (Module 4)
│   ├── httpapi/                   # HTTP handlers & response helpers
│   ├── middleware/                # RequestID, Logging, Recovery, JWT (Module 4)
│   ├── provider/
│   │   ├── blockchain/            # Ethereum / Klever adapters
│   │   └── pricing/               # CoinGecko + mock price provider
│   ├── repository/postgres/       # Repository implementations
│   │   └── refresh_token_repository.go  # (Module 4)
│   ├── service/
│   │   ├── auth_service.go        # Register/Login/Refresh/Logout (Module 4)
│   │   └── portfolio_service.go
│   ├── snapshot/                  # Snapshot runner (pipeline)
│   ├── watcher/                   # Event watcher (pipeline + fan-out)
│   └── testutil/                  # Shared test infrastructure (Module 3)
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
├── docker-compose.yml
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
| `module-4-class1` (upcoming) | 4 — JWT + gRPC | Current work |
| `module-4-class2` (planned) | 4 — Auth observability & resilience | — |
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
