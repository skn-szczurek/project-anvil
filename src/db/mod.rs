use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error};

pub async fn connect(database_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .with_context(|| "Failed to connect to database")?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

#[derive(Debug)]
pub struct TelemetryReading {
    pub device_id: String,
    pub sensor_name: String,
    pub value: f64,
    pub topic: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub struct RawMessage {
    pub topic: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

impl TelemetryReading {
    pub async fn insert(&self, client: &Client) -> Result<()> {
        client
            .execute(
                "INSERT INTO telemetry (timestamp, device_id, sensor_name, value, topic) VALUES ($1, $2, $3, $4, $5)",
                &[&self.timestamp, &self.device_id, &self.sensor_name, &self.value, &self.topic],
            )
            .await
            .with_context(|| "Failed to insert telemetry reading")?;

        debug!(
            "Inserted telemetry: device={}, sensor={}, value={}",
            self.device_id, self.sensor_name, self.value
        );

        Ok(())
    }
}

impl RawMessage {
    pub async fn insert(&self, client: &Client) -> Result<()> {
        client
            .execute(
                "INSERT INTO raw_messages (timestamp, topic, payload) VALUES ($1, $2, $3)",
                &[&self.timestamp, &self.topic, &self.payload],
            )
            .await
            .with_context(|| "Failed to insert raw message")?;

        debug!("Inserted raw message: topic={}", self.topic);

        Ok(())
    }
}
