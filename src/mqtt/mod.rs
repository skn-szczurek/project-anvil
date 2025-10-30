use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use tokio_postgres::Client as PgClient;
use tracing::{debug, error, info};

use crate::config::MqttConfig;
use crate::mapping::MappingConfig;

pub struct MqttBridge {
    _client: AsyncClient,
    eventloop: EventLoop,
    db_client: PgClient,
    mappings: MappingConfig,
}

impl MqttBridge {
    pub async fn new(
        config: MqttConfig,
        db_client: PgClient,
        mappings: MappingConfig,
    ) -> Result<Self> {
        let mut mqttoptions = MqttOptions::new(&config.client_id, &config.host, config.port);
        mqttoptions.set_keep_alive(std::time::Duration::from_secs(30));
        mqttoptions.set_clean_session(true);

        let (client, eventloop) = AsyncClient::new(mqttoptions, 10);

        // Subscribe to topics
        let qos = match config.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce,
        };

        for topic in &config.topics {
            client
                .subscribe(topic, qos)
                .await
                .with_context(|| format!("Failed to subscribe to topic: {}", topic))?;
        }

        Ok(Self {
            _client: client,
            eventloop,
            db_client,
            mappings,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        // Set up Ctrl+C handler
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

        tokio::spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for Ctrl+C");
            let _ = shutdown_tx.send(()).await;
        });

        loop {
            tokio::select! {
                event = self.eventloop.poll() => {
                    match event {
                        Ok(notification) => {
                            if let Err(e) = self.handle_event(notification).await {
                                error!("Error handling event: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("MQTT connection error: {}", e);
                            // Wait before reconnecting
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_event(&self, event: Event) -> Result<()> {
        match event {
            Event::Incoming(Packet::Publish(publish)) => {
                let topic = &publish.topic;
                let payload = &publish.payload;

                // Log at debug level only
                debug!("Received message on topic: {}", topic);

                // Execute mappings and insert into database
                if let Err(e) = crate::mapping::execute_mappings(
                    topic,
                    payload,
                    &self.mappings,
                    &self.db_client,
                )
                .await
                {
                    error!("Failed to execute mappings: {}", e);
                }
            }
            Event::Incoming(Packet::ConnAck(_)) => {
                info!("Connected to MQTT broker");
            }
            Event::Incoming(Packet::SubAck(_)) => {
                info!("Successfully subscribed to topic");
            }
            Event::Incoming(_) => {
                // Ignore other incoming packets
            }
            Event::Outgoing(_) => {
                // Ignore outgoing packets
            }
        }

        Ok(())
    }
}
