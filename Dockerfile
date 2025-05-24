# -------- Build Stage --------
FROM debian:bookworm-slim AS builder

RUN apt-get update && \
    apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo build --release || true

COPY . .
RUN cargo build --release

# -------- Runtime Stage --------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    libssl3 \
    libc6 \
    libgcc1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

COPY --from=builder /app/target/release/message-gateway .
COPY --chown=appuser:appuser config.toml .

RUN chown -R appuser:appuser /app

USER appuser

ENV CONFIG_PATH=/app/config.toml \
    CERT_PATH=/app/certs \
    RUST_LOG=info

CMD ["/app/message-gateway"]
