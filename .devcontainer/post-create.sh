#!/bin/bash
set -e

echo "Setting up Anvil Rust development environment..."

# Check Docker services status
echo "Checking Docker services..."
echo "✓ TimescaleDB: $(docker ps --filter "name=anvil-timescaledb" --format "{{.Status}}" | head -n1)"
echo "✓ Grafana: $(docker ps --filter "name=anvil-grafana" --format "{{.Status}}" | head -n1)"
echo "✓ NanoMQ: $(docker ps --filter "name=anvil-nanomq" --format "{{.Status}}" | head -n1)"
echo ""

# Verify Rust installation
echo "Verifying Rust installation..."
if command -v cargo >/dev/null 2>&1; then
    echo "✓ Cargo version: $(cargo --version)"
    echo "✓ Rustc version: $(rustc --version)"
    echo "✓ Rustup version: $(rustup --version)"
else
    echo "❌ Cargo not found in PATH"
    exit 1
fi

# Install additional useful Rust development tools
echo "Installing additional Rust development tools..."
cargo install cargo-expand 2>/dev/null || echo "cargo-expand already installed or installation failed"

# Verify project structure
echo "Verifying project setup..."
cd /workspace
if [ -f "Cargo.toml" ]; then
    echo "✓ Found Cargo.toml - running initial check..."
    cargo check || echo "⚠️  Initial cargo check failed - dependencies may need to be resolved"

    # Display project information
    echo ""
    echo "📋 Project Information:"
    echo "   Name: $(grep '^name = ' Cargo.toml | cut -d'"' -f2 || echo 'Unknown')"
    echo "   Version: $(grep '^version = ' Cargo.toml | cut -d'"' -f2 || echo 'Unknown')"
    echo ""

else
    echo "⚠️  No Cargo.toml found - this doesn't appear to be a Rust project"
fi

echo "✅ Setup complete!"
