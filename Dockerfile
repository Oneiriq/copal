# syntax=docker/dockerfile:1
# Build stage: locked dependencies, release profile. The private git
# dependencies fetch through a BuildKit secret (a read token), which
# never lands in a layer; without one, the build works only where the
# dependency cache is already warm.
FROM rust:1-trixie AS build
WORKDIR /src
COPY . .
RUN --mount=type=secret,id=oneiriq_token \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    set -e; \
    if [ -s /run/secrets/oneiriq_token ]; then \
        token="$(cat /run/secrets/oneiriq_token)"; \
        git config --global \
            url."https://x-access-token:${token}@github.com/Oneiriq/".insteadOf \
            "ssh://git@github.com/Oneiriq/"; \
        git config --global --add \
            url."https://x-access-token:${token}@github.com/Oneiriq/".insteadOf \
            "https://github.com/Oneiriq/"; \
    fi; \
    CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --release --locked -p copal-server; \
    cp target/release/copal-server /usr/local/bin/copal-server

# Runtime stage: slim, non-root, no toolchain.
FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 copal \
    && useradd --system --uid 10001 --gid copal --home /data copal \
    && mkdir -p /data/blobs \
    && chown -R copal:copal /data

COPY --from=build /usr/local/bin/copal-server /usr/local/bin/copal-server

USER copal
WORKDIR /data
ENV COPAL_BIND=0.0.0.0:8080 \
    COPAL_BLOB_ROOT=/data/blobs

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD curl -fsS http://127.0.0.1:8080/readyz || exit 1

ENTRYPOINT ["/usr/local/bin/copal-server"]
