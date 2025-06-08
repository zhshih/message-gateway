use crate::config::EdgeConfig;
use anyhow::{self, Context};
use futures_util::StreamExt as _;
use log::{debug, error, info, warn};
use rand::Rng;
use redis::aio::MultiplexedConnection;
use redis::cmd;
use redis::{AsyncCommands, Client, ClientTlsConfig, TlsCertificates};
use std::collections::HashSet;
use std::sync::Arc;
use std::{
    fs::File,
    io::{BufReader, Read},
};
use tokio::sync::{Mutex, RwLock, mpsc, watch};
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

const DEFAULT_BACKOFF_SEC: u64 = 2;
const DEFAULT_HEARTBEAT_INTERVAL: u64 = 10;

#[derive(Clone)]
pub struct RedisClient {
    client: Client,
    subscribed_topics: Arc<Mutex<HashSet<String>>>,
    event_tx: mpsc::Sender<(String, String)>,
    base_backoff_sec: Arc<RwLock<u64>>,
    is_connected_tx: watch::Sender<bool>,
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
        let (is_connected_tx, _) = watch::channel(false);
        let redis_client = RedisClient {
            client,
            subscribed_topics: Arc::new(Mutex::new(HashSet::new())),
            event_tx: tx,
            base_backoff_sec: Arc::new(RwLock::new(DEFAULT_BACKOFF_SEC)),
            is_connected_tx,
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

        debug!("Published to {} to {}", payload, topic);
        Ok(())
    }

    pub fn spawn_heartbeat(self: Arc<Self>, shutdown_token: Arc<CancellationToken>) {
        let client = self.client.clone();
        let base_backoff = self.base_backoff_sec.clone();

        tokio::spawn(async move {
            loop {
                let alive = match client.get_multiplexed_tokio_connection().await {
                    Ok(mut conn) => is_connection_alive(&mut conn).await,
                    Err(e) => {
                        warn!("Redis heartbeat failed to get connection: {e}");
                        false
                    }
                };

                let _ = self.is_connected_tx.send(alive);
                if alive {
                    info!("Redis heartbeat: alive = {alive}");

                    let mut backoff = base_backoff.write().await;
                    *backoff = DEFAULT_BACKOFF_SEC;
                } else {
                    warn!("Redis heartbeat: alive = {alive}");

                    let max_sleep = *base_backoff.read().await;
                    let jitter = rand::rng().random_range(0..=max_sleep);
                    let next_heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL + jitter;
                    info!("Retrying heartbeat in {next_heartbeat_interval} seconds...");
                    sleep(Duration::from_secs(jitter)).await;

                    let mut backoff = base_backoff.write().await;
                    *backoff = std::cmp::min(*backoff * 2, 180);
                }

                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        info!("Heartbeat task shutting down");
                        break;
                    }
                    _ = sleep(Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL)) => {}
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

                match self.connect_and_listen(shutdown_token.clone()).await {
                    Ok(_) => {}
                    Err(e) => {
                        let max_sleep = *self.base_backoff_sec.read().await;
                        let jitter = rand::rng().random_range(0..=max_sleep);
                        error!("Redis error: {e}. Retrying in {jitter}s...");
                        sleep(Duration::from_secs(jitter)).await;

                        let mut backoff = self.base_backoff_sec.write().await;
                        *backoff = std::cmp::min(*backoff * 2, 180);
                    }
                }
            }
        });
    }

    pub fn start_bridging_task(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<(String, String)>,
        shutdown_token: Arc<CancellationToken>,
    ) {
        tokio::spawn(async move {
            info!("Launching bridging task");

            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => {
                        debug!("Bridging task received shutdown signal");
                        break;
                    }
                    Some((topic, msg)) = rx.recv() => {
                        info!("Bridging {topic} and {msg}");
                        if let Err(e) = self.publish(&topic, &msg).await {
                            error!("Failed to bridge to topic {topic}: {e}");
                        }
                    }
                    else => break,
                }
            }
        });
    }

    async fn connect_and_listen(
        &self,
        shutdown_token: Arc<CancellationToken>,
    ) -> anyhow::Result<()> {
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
        let mut rx = self.is_connected_tx.subscribe();
        loop {
            tokio::select! {
                maybe_msg = pubsub_stream.next() => {
                    match maybe_msg {
                        Some(msg) => {
                            debug!("Message received: {:?}", msg);
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
                        None => {
                            warn!("PubSub stream ended");
                            break;
                        }
                    }
                }
                changed = rx.changed() => {
                    match changed {
                        Ok(_) => {
                            let connected = *rx.borrow();
                            if !connected {
                                warn!("Redis connection lost. Exiting message listener");
                                break;
                            }
                        }
                        Err(_) => {
                            error!("is_connected watch channel closed");
                            break;
                        }
                    }
                }
                _ = shutdown_token.cancelled() => {
                    info!("Shutdown token triggered. Exiting PubSub listener.");
                    break;
                }
            }
        }

        Err(anyhow::anyhow!(
            "PubSub connection closed or shutdown received"
        ))
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
