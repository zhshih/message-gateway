use crate::config::CloudConfig;
use crate::messenger::sink::SinkPublisher;
use anyhow;
use log::{debug, error, info};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MqttClient {
    client: AsyncClient,
    event_tx: mpsc::Sender<(String, String)>,
}

impl MqttClient {
    pub fn new(cfg: CloudConfig) -> (Self, EventLoop, mpsc::Receiver<(String, String)>) {
        let client_id = cfg.client_id.clone();
        let broker = cfg.broker.clone();
        let port = cfg.port.clone();
        let mut mqtt_opts = MqttOptions::new(&client_id, &broker, port);
        mqtt_opts.set_keep_alive(Duration::from_secs(60));
        mqtt_opts.set_credentials(cfg.username.clone(), cfg.password.clone());

        let (client, eventloop) = AsyncClient::new(mqtt_opts, 10);
        let (tx, rx) = mpsc::channel::<(String, String)>(32);

        let service = MqttClient {
            client,
            event_tx: tx,
        };
        (service, eventloop, rx)
    }

    pub fn event_sender(&self) -> mpsc::Sender<(String, String)> {
        self.event_tx.clone()
    }

    pub fn client(&self) -> AsyncClient {
        return self.client.clone();
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> anyhow::Result<()> {
        for topic in topics {
            debug!("Subscribing to {} from cloud", topic);

            self.client
                .subscribe(&topic, QoS::AtMostOnce)
                .await
                .map_err(|e| {
                    error!("Failed to subscribe to topic: {e}");
                    anyhow::anyhow!(e)
                })?;

            info!("Subscribed to {} from cloud", topic);
        }
        Ok(())
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        debug!("Publishing to {} to {}", payload, topic);

        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await
            .map_err(|e| {
                error!("Failed to publish to topic: {e}");
                anyhow::anyhow!(e)
            })?;

        info!("Published to {} to {}", payload, topic);
        Ok(())
    }

    pub async fn handle_events(
        client: AsyncClient,
        mut eventloop: EventLoop,
        event_tx: mpsc::Sender<(String, String)>,
        topics: Vec<String>,
        shutdown_token: Arc<CancellationToken>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    debug!("Shutdown signal received");
                    break;
                }
                result = eventloop.poll() => {
                    match result {
                        Ok(event) => {
                            debug!("Event received: {event:?}");

                            match event {
                                Event::Incoming(Packet::Publish(p)) => {
                                    match String::from_utf8(p.payload.to_vec()) {
                                        Ok(payload) => {
                                            debug!("Received payload: {}, from {}", payload, p.topic);
                                            if let Err(e) = event_tx.send((p.topic, payload)).await {
                                                error!("Failed to send event: {e}");
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to parse payload as UTF-8: {e}");
                                        }
                                    }
                                }
                                Event::Incoming(Packet::ConnAck(_)) => {
                                    for topic in &topics {
                                        if let Err(e) = client.subscribe(topic, QoS::AtMostOnce).await {
                                            error!("Failed to re-subscribe to topic {topic}: {e}");
                                        } else {
                                            info!("Re-subscribed to topic {topic}");
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(err) => {
                            error!("Event loop error: {err:?}");
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl SinkPublisher for MqttClient {
    async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        self.publish(topic, payload).await
    }
}
