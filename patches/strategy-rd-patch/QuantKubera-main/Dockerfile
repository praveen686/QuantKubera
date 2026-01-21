# Build stage
FROM rust:1.75-slim-bookworm as builder

WORKDIR /usr/src/kubera
COPY . .

# Install dependencies for build
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Build optimized binary
RUN cargo build --release -p kubera-runner

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/kubera/target/release/kubera-runner /app/kubera-runner
COPY --from=builder /usr/src/kubera/crates/kubera-connectors/scripts/zerodha_auth.py /app/zerodha_auth.py

# Install python dependencies for zerodha sidecar
RUN pip3 install kiteconnect pyotp --break-system-packages

ENV RUST_LOG=info

# Default entrypoint
ENTRYPOINT ["/app/kubera-runner"]
