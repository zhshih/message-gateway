use crate::config::EdgeConfig;
use crate::messenger::sink::SinkPublisher;
use anyhow::{self, Context};
use futures_util::StreamExt as _;
use log::{debug, error, info, warn};
use redis::aio::MultiplexedConnection;
use redis::cmd;
use redis::{AsyncCommands, Client, ClientTlsConfig, TlsCertificates};
use std::collections::HashSet;
use std::sync::Arc;
use std::{
    fs::File,
    io::{BufReader, Read},
};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct RedisClient {
    client: Client,
    subscribed_topics: Arc<Mutex<HashSet<String>>>,
    event_tx: mpsc::Sender<(String, String)>,
}

impl RedisClient {
    pub async fn new(cfg: EdgeConfig) -> anyhow::Result<(Self, mpsc::Receiver<(String, String)>)> {
        let connection_url = Self::build_redis_connection(&cfg);

        let client = if cfg.auth.mtls_enabled {
            info!("mTLS auth enabled");

            let mut ca = Vec::new();
            BufReader::new(
                File::open(cfg.auth.mtls.as_ref().unwrap().ca_file.as_str())
                    .expect("Failed to find CA certificate"),
            )
            .read_to_end(&mut ca)
            .expect("Failed to read CA certificate");

            let mut cert = Vec::new();
            BufReader::new(
                File::open(cfg.auth.mtls.as_ref().unwrap().cert_file.as_str())
                    .expect("Failed to find client certificate"),
            )
            .read_to_end(&mut cert)
            .expect("Failed to read client certificate");

            let mut key = Vec::new();
            BufReader::new(
                File::open(cfg.auth.mtls.as_ref().unwrap().key_file.as_str())
                    .expect("Failed to find client key"),
            )
            .read_to_end(&mut key)
            .expect("Failed to read client key");

            Client::build_with_tls(
                connection_url,
                TlsCertificates {
                    client_tls: Some(ClientTlsConfig {
                        client_cert: cert,
                        client_key: key,
                    }),
                    root_cert: Some(ca),
                },
            )
            .expect("Unable to build Redis client")
        } else {
            info!("Basic auth enabled");
            Client::open(connection_url).context("Failed to open Redis connection")?
        };

        let (tx, rx) = mpsc::channel::<(String, String)>(32);

        let redis_client = RedisClient {
            client,
            subscribed_topics: Arc::new(Mutex::new(HashSet::new())),
            event_tx: tx,
        };

        Ok((redis_client, rx))
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> anyhow::Result<()> {
        let mut topic_set = self.subscribed_topics.lock().await;
        for topic in topics {
            topic_set.insert(topic);
        }
        Ok(())
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        debug!("Publishing to {} to {}", payload, topic);

        let mut publish_conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get multiplexed async connection")?;

        let _: () = publish_conn.publish(topic, payload).await?;

        info!("Published to {} to {}", payload, topic);
        Ok(())
    }

    pub fn spawn_heartbeat(self: Arc<Self>, shutdown_token: Arc<CancellationToken>) {
        let client = self.client.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        info!("Heartbeat task shutting down");
                        break;
                    }
                    _ = sleep(Duration::from_secs(5)) => {
                        match client.get_multiplexed_tokio_connection().await {
                            Ok(mut conn) => {
                                let alive = is_connection_alive(&mut conn).await;
                                info!("Redis heartbeat: alive = {alive}");
                            }
                            Err(e) => {
                                warn!("Redis heartbeat failed to get connection: {e}");
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn start_handling_messages(self: Arc<Self>, shutdown_token: Arc<CancellationToken>) {
        tokio::spawn(async move {
            loop {
                if shutdown_token.is_cancelled() {
                    break;
                }

                match self.connect_and_listen().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Redis pubsub error: {e}. Retrying in 2s...");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
        info!("Handling message task is terminated");
    }

    async fn connect_and_listen(&self) -> anyhow::Result<()> {
        let mut pubsub_conn = self.client.get_async_pubsub().await?;
        let topics: Vec<_> = self
            .subscribed_topics
            .lock()
            .await
            .iter()
            .cloned()
            .collect();

        for topic in &topics {
            debug!("Subscribing to {} from edge", topic);
            pubsub_conn.subscribe(topic).await?;
            info!("Subscribed to {} from edge", topic);
        }

        let mut pubsub_stream = pubsub_conn.on_message();
        while let Some(msg) = pubsub_stream.next().await {
            debug!("Message received: {msg:?}");
            match msg.get_payload::<String>() {
                Ok(payload) => {
                    let channel = msg.get_channel_name();
                    debug!("Received payload: {}, from {}", payload, channel);
                    if let Err(e) = self.event_tx.send((channel.into(), payload)).await {
                        error!("Failed to send event: {e}");
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to get payload: {}", e);
                }
            }
        }

        Err(anyhow::anyhow!("PubSub connection lost"))
    }

    fn build_redis_connection(cfg: &EdgeConfig) -> String {
        if cfg.auth.basic_enabled && !cfg.auth.mtls_enabled {
            format!(
                "redis://{}:{}@{}:{}/{}",
                cfg.auth.basic.as_ref().unwrap().username,
                cfg.auth.basic.as_ref().unwrap().password,
                cfg.broker,
                cfg.port,
                cfg.db
            )
        } else if cfg.auth.mtls_enabled {
            format!(
                "rediss://{}:{}@{}:{}/{}",
                cfg.auth.basic.as_ref().unwrap().username,
                cfg.auth.basic.as_ref().unwrap().password,
                cfg.broker,
                cfg.port,
                cfg.db
            )
        } else {
            format!("redis://{}:{}/{}", cfg.broker, cfg.port, cfg.db)
        }
    }
}

async fn is_connection_alive(conn: &mut MultiplexedConnection) -> bool {
    cmd("PING").query_async::<String>(conn).await.is_ok()
}

#[async_trait::async_trait]
impl SinkPublisher for RedisClient {
    async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        self.publish(topic, payload).await
    }
}
