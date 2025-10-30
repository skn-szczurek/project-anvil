use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub mqtt: MqttConfig,
    pub database: DatabaseConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub topics: Vec<String>,
    pub qos: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mqtt: MqttConfig {
                host: "localhost".to_string(),
                port: 1883,
                client_id: "anvil".to_string(),
                topics: vec![
                    "test/telemetry".to_string(),
                    "debug/diagnostics/#".to_string(),
                    "diagnostics/logs/+".to_string(),
                    "telemetry/#".to_string(),
                ],
                qos: 0,
            },
            database: DatabaseConfig {
                url: "postgresql://admin:admin@localhost:5432/metrics".to_string(),
            },
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let config: Config =
            toml::from_str(&contents).with_context(|| "Failed to parse config file")?;

        Ok(config)
    }
}
