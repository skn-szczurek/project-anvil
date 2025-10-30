#!/bin/bash
set -e

# Create the TimescaleDB extension
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

    -- Create telemetry table for simple sensor data
    -- Stores individual sensor readings like {"temperature": 80, "ph": 2.4}
    CREATE TABLE IF NOT EXISTS telemetry (
        timestamp TIMESTAMPTZ NOT NULL,
        id SERIAL NOT NULL,
        device_id TEXT NOT NULL,
        sensor_name TEXT NOT NULL,
        value DOUBLE PRECISION NOT NULL,
        topic TEXT NOT NULL,
        PRIMARY KEY (timestamp, id)
    );

    -- Create raw messages table for audit trail
    CREATE TABLE IF NOT EXISTS raw_messages (
        timestamp TIMESTAMPTZ NOT NULL,
        id SERIAL NOT NULL,
        topic TEXT NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (timestamp, id)
    );

    -- Convert to hypertables for time-series optimization
    SELECT create_hypertable('telemetry', 'timestamp', if_not_exists => TRUE);
    SELECT create_hypertable('raw_messages', 'timestamp', if_not_exists => TRUE);

    -- Create indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_telemetry_device_id ON telemetry (device_id);
    CREATE INDEX IF NOT EXISTS idx_telemetry_sensor_name ON telemetry (sensor_name);
    CREATE INDEX IF NOT EXISTS idx_telemetry_device_sensor ON telemetry (device_id, sensor_name);
    CREATE INDEX IF NOT EXISTS idx_raw_messages_topic ON raw_messages (topic);

    -- Configure proper authentication
    ALTER USER admin WITH PASSWORD 'admin';
EOSQL

# Update pg_hba.conf to allow connections from the network
cat > "${PGDATA}/pg_hba.conf" <<EOF
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     trust
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
host    all             all             0.0.0.0/0               md5
EOF

chmod 600 "${PGDATA}/pg_hba.conf"
