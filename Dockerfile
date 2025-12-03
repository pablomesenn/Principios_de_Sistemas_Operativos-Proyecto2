# Dockerfile for Mini-Spark
# Multi-stage build for optimizing image size

# ============ Compilation Stage ============
FROM rust:1.83-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# First copy configuration files (for caching dependencies)
COPY Cargo.toml Cargo.lock* ./
COPY master/Cargo.toml ./master/
COPY worker/Cargo.toml ./worker/
COPY client/Cargo.toml ./client/
COPY common/Cargo.toml ./common/

# Create dummy files for compiling dependencies
RUN mkdir -p master/src worker/src client/src common/src && \
    echo "fn main() {}" > master/src/main.rs && \
    echo "fn main() {}" > worker/src/main.rs && \
    echo "fn main() {}" > client/src/main.rs && \
    echo "" > common/src/lib.rs

# Compile dependencies (cached)
RUN cargo build --release || true

# Copy real source code
COPY master/src ./master/src
COPY worker/src ./worker/src
COPY client/src ./client/src
COPY common/src ./common/src

# Recompile with real code
RUN touch master/src/main.rs worker/src/main.rs client/src/main.rs common/src/lib.rs && \
    cargo build --release

# ============ Master Image ============
FROM debian:bookworm-slim AS master

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create necessary directories
RUN mkdir -p /tmp/minispark/state /tmp/minispark/spill /app/data

COPY --from=builder /app/target/release/master /app/master

EXPOSE 8080

ENV RUST_LOG=info

CMD ["/app/master"]

# ============ Worker Image ============
FROM debian:bookworm-slim AS worker

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create necessary directories
RUN mkdir -p /tmp/minispark/state /tmp/minispark/spill /app/data

COPY --from=builder /app/target/release/worker /app/worker

EXPOSE 9000 10000

ENV MASTER_URL=http://master:8080
ENV WORKER_PORT=9000
ENV WORKER_THREADS=4
ENV CACHE_MAX_MB=128
ENV LOG_LEVEL=INFO
ENV LOG_FORMAT=text

CMD ["/app/worker"]

# ============ Client Image ============
FROM debian:bookworm-slim AS client

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create necessary directories
RUN mkdir -p /tmp/minispark /app/data

COPY --from=builder /app/target/release/client /app/client

ENV MASTER_URL=http://master:8080

ENTRYPOINT ["/app/client"]
CMD ["--help"]
