# Build stage: locked dependencies, release profile.
FROM rust:1.90-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p copal-server

# Runtime stage: slim, non-root, no toolchain.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 copal \
    && useradd --system --uid 10001 --gid copal --home /data copal \
    && mkdir -p /data/blobs \
    && chown -R copal:copal /data

COPY --from=build /src/target/release/copal-server /usr/local/bin/copal-server

USER copal
WORKDIR /data
ENV COPAL_BIND=0.0.0.0:8080 \
    COPAL_BLOB_ROOT=/data/blobs

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD curl -fsS http://127.0.0.1:8080/readyz || exit 1

ENTRYPOINT ["/usr/local/bin/copal-server"]
