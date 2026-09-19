# syntax=docker/dockerfile:1

# ---- Stage 1: build the release binary ----
FROM rust:1-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from source changes: copy only
# the manifests first, build a throwaway main.rs against them, then copy
# the real source and rebuild. Speeds up incremental image rebuilds a lot
# since `cargo build --release` for all of bus's dependencies is the
# expensive part.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
# Touch main.rs so cargo doesn't reuse the stale dummy-build artifact's
# timestamp and skip recompiling the real source.
RUN touch src/main.rs && cargo build --release

# ---- Stage 2: minimal runtime image ----
FROM debian:bookworm-slim AS runtime

# ca-certificates: harmless/likely-useful baseline for a network service;
# no TLS termination happens in this server itself, but keeping the system
# CA bundle present avoids surprises if that ever changes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /home/bus --shell /usr/sbin/nologin bus

COPY --from=builder /build/target/release/bus /usr/local/bin/bus

# Sled data directory — mount a volume here to persist messages/users/ACL
# across container restarts (matches `bus serve`'s `--data-dir` default of
# `./data`, made absolute and owned by the unprivileged `bus` user here).
ENV BUS_DATA_DIR=/data
RUN mkdir -p /data && chown bus:bus /data
VOLUME ["/data"]

USER bus
WORKDIR /home/bus

# Default ntfy-style port per Config::bind's default (127.0.0.1:8080) —
# rebound to 0.0.0.0 here since 127.0.0.1 wouldn't be reachable from
# outside the container.
EXPOSE 8080

ENTRYPOINT ["bus", "serve"]
CMD ["--bind", "0.0.0.0:8080", "--data-dir", "/data"]
