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
| 3 | **Professional Testing** — `testing`/`testify`/`mock`, integration & concurrency tests, benchmarking & profiling | 3h | This repo — **current state** |
| 4 | **Networking & Distributed Services** — TCP/UDP, REST APIs, JWT, RabbitMQ/Kafka, gRPC, streaming, pooling, retries | 6h | Upcoming — same repo |
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

Default API endpoints:

```
GET  /health
GET  /users/{id}/portfolio
GET  /users/{id}/wallets
POST /users/{id}/wallets
GET  /debug/panic         # educational only — demonstrates recovery middleware
```

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

## Project Layout

```
portfolio-api/
├── cmd/
│   ├── api/                       # REST API entrypoint
│   ├── event-watcher/             # Ethereum event monitor
│   └── snapshot-runner/           # Wallet balance snapshot generator
├── internal/
│   ├── concurrent/                # Reusable concurrency helpers (Module 2)
│   │   ├── workerpool.go          # RunWorkerPool[I, O]
│   │   ├── fanout.go              # FanOut[T] + Merge[T]
│   │   └── pipeline.go            # Stage[I, O] + Generate[T]
│   ├── config/                    # Environment-driven configuration
│   ├── contracts/                 # Port interfaces (repositories & providers)
│   ├── domain/                    # Entities, sentinel errors, AppError
│   ├── httpapi/                   # HTTP handlers & response helpers
│   ├── middleware/                # RequestID, Logging, Recovery
│   ├── provider/
│   │   ├── blockchain/            # Ethereum / Klever adapters
│   │   └── pricing/               # CoinGecko + mock price provider
│   ├── repository/postgres/       # Repository implementations
│   ├── snapshot/                  # Snapshot runner (pipeline)
│   ├── watcher/                   # Event watcher (pipeline + fan-out)
│   └── testutil/                  # Shared test infrastructure (Module 3)
│       ├── anvil/                 # testcontainers wrapper for Anvil
│       ├── ethutil/               # JSON-RPC client + Anvil + ERC-20 helpers
│       ├── mocks/                 # testify/mock implementations
│       ├── postgres/              # Postgres helper (pre-existing)
│       └── testenv/               # TestMain-level bootstrap (DB → chain)
├── migrations/                    # SQL migrations (embedded via //go:embed)
├── test/
│   ├── infrastructure/anvil/      # Custom Anvil Dockerfile
│   └── integration/               # End-to-end tests (build tag: integration)
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
| `module-3` (upcoming) | 3 — Professional Testing | Current work |
| `module-4` (planned) | 4 — Networking & Distributed Services | — |
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
