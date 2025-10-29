# Simplified Rust-only devcontainer for Anvil project
FROM rust:1.89-slim-bookworm

# Set environment
ENV DEBIAN_FRONTEND=noninteractive
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo

# Install essential system dependencies for Rust development
RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \
    pkg-config \
    libssl-dev \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create vscode user
RUN groupadd -g 1000 vscode && \
    useradd -m -u 1000 -g 1000 -s /bin/bash vscode

# Set up workspace
WORKDIR /workspace
RUN chown -R vscode:vscode /workspace

# Switch to vscode user
USER vscode

# Install additional Rust tools for development
RUN rustup component add clippy rustfmt && \
    cargo install cargo-watch
