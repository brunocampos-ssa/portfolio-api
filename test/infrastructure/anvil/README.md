# Custom Anvil Image (Integration Tests)

This folder hosts the Dockerfile used by integration tests that need a local
Ethereum-compatible chain. It wraps the [Foundry](https://book.getfoundry.sh/)
`anvil` binary with `socat` so the public TCP port stays consistent while the
underlying `anvil` process is bound to `127.0.0.1`.

## Why a custom image?

- `anvil` alone is sufficient for trivial tests, but our integration tests
  drive it via `testcontainers-go`. Having a tiny wrapper we control removes a
  class of flaky-behaviour bugs ("works on my laptop, fails in CI") that come
  from relying on base-image defaults that change upstream.
- `socat` is used to keep the advertised port stable (`8545`) regardless of
  `anvil`'s real listener. That means we can swap `anvil` flags, change
  internal ports, or run fork/non-fork variants without breaking test code.

## Environment variables

| Variable              | Default | Purpose                                |
| --------------------- | ------- | -------------------------------------- |
| `ANVIL_PORT`          | `8545`  | Public port tests connect to.          |
| `ANVIL_PORT_INTERNAL` | `9000`  | Port `anvil` actually binds to.        |
| `ANVIL_ARGS`          | empty   | Extra args forwarded to `anvil`.       |

## How tests use it

`internal/testutil/anvil` builds and starts this image via
`testcontainers-go`. See that package for the helper API.
