# =============================================================================
# Custom Anvil image used by integration tests.
#
# Base image:  Foundry stable, which ships the `anvil` local chain binary.
# Extra tool:  socat — lets us expose Anvil on a public port while keeping
#              anvil itself bound to 127.0.0.1 (mimics production setups where
#              the node is behind a local proxy).
#
# Runtime flow (see ENTRYPOINT below):
#   socat listens on ANVIL_PORT  (default 8545)
#   socat forwards to 127.0.0.1:ANVIL_PORT_INTERNAL (default 9000)
#   anvil binds to 127.0.0.1:ANVIL_PORT_INTERNAL
#
# Extra anvil flags can be injected via ANVIL_ARGS (e.g. `--block-time 1`,
# `--fork-url https://…`). Tests typically leave it empty — determinism
# matters more than block times.
# =============================================================================
FROM ghcr.io/foundry-rs/foundry:stable

# Install socat using APT (Debian-based)
USER root
RUN apt-get update && \
    apt-get install -y socat && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set options for anvil
ENV ANVIL_ARGS=
# Default port
ENV ANVIL_PORT=8545
# Internal port for anvil to bind to
ENV ANVIL_PORT_INTERNAL=9000

# Entrypoint script socat + anvil
ENTRYPOINT ["/bin/sh", "-c", "\
    echo 'Starting socat on ${ANVIL_PORT} → ${ANVIL_PORT_INTERNAL}' && \
    socat TCP-LISTEN:${ANVIL_PORT},fork,reuseaddr TCP:127.0.0.1:${ANVIL_PORT_INTERNAL} & \
    echo 'Starting anvil on ${ANVIL_PORT_INTERNAL}' && \
    anvil --host 127.0.0.1 --port ${ANVIL_PORT_INTERNAL} $ANVIL_ARGS \
    "]
