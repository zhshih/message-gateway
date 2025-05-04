use crate::config::EdgeConfig;
use crate::messenger::sink::SinkPublisher;
use anyhow::{self, Context};
use futures_util::StreamExt as _;
use log::{debug, error, info};
use redis::aio::PubSub;
use redis::{AsyncCommands, Client};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct RedisClient {
    client: Client,
    event_tx: mpsc::Sender<(String, String)>,
}

impl RedisClient {
    pub async fn new(cfg: EdgeConfig) -> anyhow::Result<(Self, mpsc::Receiver<(String, String)>)> {
        let connection_url = format!("redis://{}:{}/{}", cfg.broker, cfg.port, cfg.db);

        let client = Client::open(connection_url).context("Failed to open Redis connection")?;
        let (tx, rx) = mpsc::channel::<(String, String)>(32);

        let redis_client = RedisClient {
            client,
            event_tx: tx,
        };

        Ok((redis_client, rx))
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> anyhow::Result<PubSub> {
        let mut pubsub_conn = self.client.get_async_pubsub().await?;
        for topic in topics {
            info!("Subscribing to {} from edge", topic);

            pubsub_conn.subscribe(&topic).await?;
            info!("Subscribed to {} from edge", topic);
        }
        Ok(pubsub_conn)
    }

    pub fn message_sender(&self) -> mpsc::Sender<(String, String)> {
        self.event_tx.clone()
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        info!("Publishing to {} to {}", payload, topic);

        let mut publish_conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get multiplexed async connection")?;

        let _: () = publish_conn.publish(&topic, payload).await?;

        info!("Published to {} to {}", payload, topic);
        Ok(())
    }

    pub async fn handle_messages(
        mut pubsub_conn: PubSub,
        event_tx: mpsc::Sender<(String, String)>,
        shutdown_token: Arc<CancellationToken>,
    ) {
        let mut pubsub_stream = pubsub_conn.on_message();

        while !shutdown_token.is_cancelled() {
            tokio::select! {
                msg = pubsub_stream.next() => {
                    match msg {
                        Some(msg) => {
                            debug!("Message received: {msg:?}");
                            match msg.get_payload::<String>() {
                                Ok(payload) => {
                                    let channel = msg.get_channel_name();
                                    debug!("Received payload: {}, from {}", channel, payload);
                                    if let Err(e) = event_tx.send((channel.into(), payload)).await {
                                        error!("Failed to send event: {e}");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to get payload: {}", e);
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                },
                _ = shutdown_token.cancelled() => {
                    debug!("Shutdown signal received");
                    break;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl SinkPublisher for RedisClient {
    async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        self.publish(topic, payload).await
    }
}
