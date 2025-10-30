use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;

mod config;
mod db;
mod mapping;
mod mqtt;

use config::Config;
use mapping::MappingConfig;

#[derive(Parser)]
#[command(name = "anvil")]
#[command(version = "0.1.0")]
#[command(about = "Device Telemetry Monitoring Bridge - MQTT to TimescaleDB", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the telemetry bridge
    Start {
        /// Path to configuration file
        #[arg(short, long, default_value = "anvil.toml")]
        config: String,

        /// Path to mappings file
        #[arg(short, long, default_value = "mappings.yaml")]
        mappings: String,

        /// MQTT broker host
        #[arg(long)]
        mqtt_host: Option<String>,

        /// MQTT broker port
        #[arg(long)]
        mqtt_port: Option<u16>,

        /// PostgreSQL connection string
        #[arg(long)]
        db_url: Option<String>,
    },

    /// Generate a sample configuration file
    Config {
        /// Output path for configuration file
        #[arg(short, long, default_value = "anvil.toml")]
        output: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            config,
            mappings,
            mqtt_host,
            mqtt_port,
            db_url,
        } => {
            start_bridge(config, mappings, mqtt_host, mqtt_port, db_url).await?;
        }
        Commands::Config { output } => {
            generate_config(&output)?;
        }
    }

    Ok(())
}

async fn start_bridge(
    config_path: String,
    mappings_path: String,
    mqtt_host_override: Option<String>,
    mqtt_port_override: Option<u16>,
    db_url_override: Option<String>,
) -> Result<()> {
    println!("{}", "Anvil Telemetry Bridge".bright_cyan().bold());
    println!("{}", "======================".bright_cyan());
    println!();

    // Load configuration
    let mut config = Config::load(&config_path)?;

    // Apply CLI overrides
    if let Some(host) = mqtt_host_override {
        config.mqtt.host = host;
    }
    if let Some(port) = mqtt_port_override {
        config.mqtt.port = port;
    }
    if let Some(url) = db_url_override {
        config.database.url = url;
    }

    println!(
        "{} {}",
        "MQTT Broker:".bright_green(),
        format!("{}:{}", config.mqtt.host, config.mqtt.port).yellow()
    );
    println!(
        "{} {} topics",
        "Subscribed to:".bright_green(),
        config.mqtt.topics.len().to_string().yellow()
    );
    for topic in &config.mqtt.topics {
        println!("  {} {}", "→".dimmed(), topic.cyan());
    }
    println!();

    // Load mappings
    let mappings = MappingConfig::load(&mappings_path)?;
    println!(
        "{} {} mappings",
        "✓ Loaded mappings:".green(),
        mappings.mappings.len().to_string().yellow()
    );
    for mapping in &mappings.mappings {
        println!(
            "  {} {} → {}",
            "→".dimmed(),
            mapping.name.cyan(),
            mapping.table.cyan()
        );
    }
    println!();

    // Initialize database connection
    let db_client = db::connect(&config.database.url).await?;
    println!("{}", "✓ Connected to TimescaleDB".green());

    // Initialize MQTT client
    let mqtt_bridge = mqtt::MqttBridge::new(config.mqtt.clone(), db_client, mappings).await?;
    println!("{}", "✓ Connected to MQTT broker".green());
    println!();

    println!(
        "{}",
        "Bridge is running. Press Ctrl+C to stop...".bright_green()
    );
    println!();

    // Run the bridge
    mqtt_bridge.run().await?;

    println!("{}", "\nShutting down...".yellow());
    Ok(())
}

fn generate_config(output_path: &str) -> Result<()> {
    let default_config = Config::default();
    let toml_string = toml::to_string_pretty(&default_config)?;

    std::fs::write(output_path, toml_string)?;

    println!(
        "{} {}",
        "✓ Configuration file generated:".green(),
        output_path.cyan()
    );

    // Also generate default mappings.yaml
    let mappings_path = "mappings.yaml";
    let default_mappings = MappingConfig::default();
    let yaml_string = serde_yaml::to_string(&default_mappings)?;

    std::fs::write(mappings_path, yaml_string)?;

    println!(
        "{} {}",
        "✓ Mappings file generated:".green(),
        mappings_path.cyan()
    );

    println!();
    println!("Edit the files and then run:");
    println!("  {}", "anvil start".yellow());

    Ok(())
}
