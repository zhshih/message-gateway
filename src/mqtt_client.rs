use crate::config::CloudConfig;
use anyhow;
use log::{debug, error, info};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MqttClient {
    cfg: CloudConfig,
    client: AsyncClient,
    event_tx: mpsc::Sender<String>,
}

impl MqttClient {
    pub fn new(cfg: CloudConfig) -> (Self, EventLoop, mpsc::Receiver<String>) {
        let client_id = cfg.client_id.clone();
        let broker = cfg.broker.clone();
        let port = cfg.port.clone();
        let mut mqtt_opts = MqttOptions::new(&client_id, &broker, port);
        mqtt_opts.set_keep_alive(Duration::from_secs(60));
        mqtt_opts.set_credentials(cfg.username.clone(), cfg.password.clone());

        let (client, eventloop) = AsyncClient::new(mqtt_opts, 10);
        let (tx, rx) = mpsc::channel(32);

        let service = MqttClient {
            cfg,
            client,
            event_tx: tx,
        };
        (service, eventloop, rx)
    }

    pub fn event_sender(&self) -> mpsc::Sender<String> {
        self.event_tx.clone()
    }

    pub async fn subscribe(&self) -> anyhow::Result<()> {
        let topic = self.cfg.sub_cloud_topic.clone();
        info!("Subscribing to {} from cloud", topic);

        self.client
            .subscribe(&topic, QoS::AtMostOnce)
            .await
            .map_err(|e| {
                error!("Failed to subscribe to topic: {e}");
                anyhow::anyhow!(e)
            })?;

        info!("Subscribed to {} from cloud", topic);
        Ok(())
    }

    pub async fn publish(&self, payload: &str) -> anyhow::Result<()> {
        let topic = self.cfg.pub_cloud_topic.clone();
        info!("Publishing to {} to {}", payload, topic);

        self.client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .await
            .map_err(|e| {
                error!("Failed to publish to topic: {e}");
                anyhow::anyhow!(e)
            })?;

        info!("Published to {} to {}", payload, topic);
        Ok(())
    }

    pub async fn handle_events(
        mut eventloop: EventLoop,
        event_tx: mpsc::Sender<String>,
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

                            if let Event::Incoming(Packet::Publish(p)) = event {
                                match String::from_utf8(p.payload.to_vec()) {
                                    Ok(payload) => {
                                        if let Err(e) = event_tx.send(payload).await {
                                            error!("Failed to send event: {e}");
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to parse payload as UTF-8: {e}");
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!("Event loop error: {err:?}");
                            break;
                        }
                    }
                }
            }
        }
    }
}
