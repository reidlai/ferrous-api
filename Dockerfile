# syntax=docker/dockerfile:1.4
# Hardened rootless FROM scratch container build for ferrous-api Axum API gateway
# Adheres strictly to AD-24 (<50MB footprint, UID 10001, static musl, Day-0 defense)

FROM rust:1.86-alpine AS builder

# hadolint ignore=DL3018
RUN apk add --no-cache musl-dev gcc g++ make cmake ca-certificates binutils

WORKDIR /usr/src/workspace

# Create unprivileged user / group files and empty mount points for scratch runtime
RUN echo "ferrous:x:10001:10001:FerrousCX API Gateway:/nonexistent:/sbin/nologin" > /etc/passwd.scratch && \
    echo "ferrous:x:10001:" > /etc/group.scratch && \
    mkdir -m 1777 /tmp_scratch && \
    mkdir -p /data_delta_scratch && \
    chown 10001:10001 /data_delta_scratch

# Copy sibling path dependency (ferrous-cqrses) and ferrous-api manifest/sources
COPY ferrous-cqrses/Cargo.toml ferrous-cqrses/Cargo.lock ./ferrous-cqrses/
COPY ferrous-cqrses/src/ ./ferrous-cqrses/src/
COPY ferrous-api/Cargo.toml ferrous-api/Cargo.lock ./ferrous-api/
COPY ferrous-api/src/ ./ferrous-api/src/

WORKDIR /usr/src/workspace/ferrous-api
ENV RUSTFLAGS="-C target-feature=+crt-static"
RUN rustup target add x86_64-unknown-linux-musl && \
    cargo build --release --locked --target x86_64-unknown-linux-musl --bin ferrous-api && \
    strip /usr/src/workspace/ferrous-api/target/x86_64-unknown-linux-musl/release/ferrous-api

FROM scratch

LABEL org.opencontainers.image.title="ferrous-api" \
      org.opencontainers.image.description="Hardened rootless FROM scratch Axum API gateway and in-process DataFusion lakehouse for FerrousCX" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.vendor="FerrousCX" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

COPY --from=builder /etc/passwd.scratch /etc/passwd
COPY --from=builder /etc/group.scratch /etc/group
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /tmp_scratch /tmp
COPY --from=builder /data_delta_scratch /data/delta
COPY --from=builder /usr/src/workspace/ferrous-api/target/x86_64-unknown-linux-musl/release/ferrous-api /ferrous-api

USER 10001:10001

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["/ferrous-api", "--healthcheck"]

ENTRYPOINT ["/ferrous-api"]
CMD []
