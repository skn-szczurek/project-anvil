# Anvil DevContainer

Complete Rust development environment for Anvil MQTT to TimescaleDB bridge with all services included.

## Quick Start (Single Command!)

```bash
# First time - builds and starts everything (takes 3-5 minutes)
devpod up . --ide none

# Connect to container
devpod ssh .

# Inside container - everything is already running!
# Just configure and run:
cp anvil.toml.example anvil.toml
# Edit anvil.toml with your settings (services are at localhost)
cargo run -- --config anvil.toml
```

## What's Included

Everything you need in one command:

- **Rust 1.89** - Latest stable Rust toolchain with clippy, rustfmt, cargo-watch
- **TimescaleDB** - PostgreSQL with TimescaleDB extension (localhost:5432)
- **Grafana** - Data visualization and dashboards (http://localhost:3000)
- **NanoMQ** - Lightweight MQTT broker (mqtt://localhost:1883)
- **System dependencies** - OpenSSL, build tools, etc.
- **Persistent volumes** - Data and build caches are preserved

## Daily Usage

```bash
# Connect (instant if container exists - all services auto-start)
devpod ssh .

# Stop everything (preserves all data and containers)
devpod stop .

# Restart everything (few seconds - services auto-start)
devpod up . --ide none

# Only rebuild when Dockerfile changes
devpod up . --ide none --recreate
```

## Service Access

All services are accessible from both inside the devcontainer and from your host machine:

- **TimescaleDB**: `localhost:5432` (user: `admin`, password: `admin`, database: `metrics`)
- **Grafana**: `http://localhost:3000` (user: `admin`, password: `admin`)
- **NanoMQ MQTT**: `mqtt://localhost:1883`
- **NanoMQ WebSocket**: `ws://localhost:8083/mqtt`
- **NanoMQ HTTP API**: `http://localhost:8081`

## Configuration

Inside the devcontainer:

```bash
# Copy example config
cp anvil.toml.example anvil.toml

# Edit configuration - use these connection strings:
# MQTT: mqtt://nanomq:1883 (or localhost:1883)
# PostgreSQL: postgresql://admin:admin@timescaledb:5432/metrics (or localhost:5432)
nano anvil.toml  # or use your preferred editor

# Run Anvil
cargo run -- --config anvil.toml
```

## Container Architecture

Everything runs in Docker Compose:

- **devcontainer** - Rust development environment with your code
- **timescaledb** - PostgreSQL 17 + TimescaleDB extension with pre-initialized schema
- **grafana** - Enterprise edition with TimescaleDB datasource pre-configured
- **nanomq** - Lightweight MQTT broker with WebSocket and HTTP API enabled

All services are networked together and start automatically when you run `devpod up`.

## Troubleshooting

- **First build slow?** Normal - downloading and building all services only happens once
- **Can't connect?** Try `devpod delete . --force` then `devpod up .`
- **Service not responding?** Check status: `docker ps` to see if all containers are healthy
- **Database connection issues?** Services might still be starting - wait for health checks (30s)
- **Port conflicts?** Ensure ports 5432, 3000, 1883, 8083, 8081 are not in use on your host
