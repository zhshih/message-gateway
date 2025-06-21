# -------- Build Stage --------
FROM rustlang/rust:nightly-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    clang \
    cmake

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo +nightly build --release

# -------- Runtime Stage --------
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/certs && \
    useradd -m appuser

WORKDIR /app

COPY --from=builder /app/target/release/message-gateway ./message-gateway
COPY config.toml ./config.toml

RUN chown -R appuser:appuser /app

USER appuser

ENV CONFIG_PATH=/app/config.toml \
    CERT_PATH=/app/certs \
    RUST_LOG=info

ENTRYPOINT ["./message-gateway"]
