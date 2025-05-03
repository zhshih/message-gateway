use crate::config::{CloudConfig, EdgeConfig};
use crate::mqtt_client::MqttClient;
use crate::redis_client::RedisClient;
use anyhow;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub async fn start_pipeline(
    cloud_cfg: CloudConfig,
    edge_cfg: EdgeConfig,
    shutdown_token: Arc<CancellationToken>,
) -> anyhow::Result<()> {
    let (cloud_client, eventloop, cloud_source_rx) = MqttClient::new(cloud_cfg.clone());
    let (edge_client, edge_source_rx) = RedisClient::new(edge_cfg.clone()).await?;

    info!("Start source task");
    let cloud_sub_topics = vec![cloud_cfg.clone().sub_cloud_topic.clone()];
    cloud_client.subscribe(cloud_sub_topics).await?;

    let edge_sub_topics = vec![edge_cfg.clone().sub_edge_topic.clone()];
    let pubsub_conn = edge_client.subscribe(edge_sub_topics).await?;

    let cloud_sub_topics = vec![cloud_cfg.clone().sub_cloud_topic.clone()];
    let cloud_source_task = tokio::spawn(MqttClient::handle_events(
        cloud_client.client(),
        eventloop,
        cloud_client.event_sender(),
        cloud_sub_topics,
        shutdown_token.clone(),
    ));

    let edge_source_task = tokio::spawn(RedisClient::handle_messages(
        pubsub_conn,
        edge_client.message_sender(),
        shutdown_token.clone(),
    ));

    info!("Start process task");
    let (cloud_processed_tx, mut cloud_processed_rx) = mpsc::channel(32);
    let cloud_process_task = spawn_processor(
        "cloud",
        cloud_source_rx,
        cloud_processed_tx,
        shutdown_token.clone(),
    );

    let (edge_processed_tx, mut edge_processed_rx) = mpsc::channel(32);
    let edge_process_task = spawn_processor(
        "edge",
        edge_source_rx,
        edge_processed_tx,
        shutdown_token.clone(),
    );

    let shutdown_token_clone = shutdown_token.clone();
    let cloud_sink_task = tokio::spawn(async move {
        info!("Launching cloud-sink task");

        loop {
            tokio::select! {
                _ = shutdown_token_clone.cancelled() => {
                    debug!("Sink task received shutdown signal.");
                    break;
                }
                maybe_msg = cloud_processed_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            debug!("Received processed message: {msg}");

                            debug!("Publishing sink message: {msg}");
                            if let Err(e) = cloud_client.publish(&msg).await {
                                error!("Failed to publish message: {:?}", e);
                            } else {
                                debug!("Published sink message: {msg}");
                            }
                        }
                        None => {
                            info!("Processed channel closed.");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Exiting cloud-sink task.");
    });

    let shutdown_token_clone = shutdown_token.clone();
    let edge_sink_task = tokio::spawn(async move {
        info!("Launching edge-sink task");

        loop {
            tokio::select! {
                _ = shutdown_token_clone.cancelled() => {
                    debug!("Sink task received shutdown signal.");
                    break;
                }
                maybe_msg = edge_processed_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            debug!("Received processed message: {msg}");

                            debug!("Publishing sink message: {msg}");
                            if let Err(e) = edge_client.publish(&msg).await {
                                error!("Failed to publish message: {:?}", e);
                            } else {
                                debug!("Published sink message: {msg}");
                            }
                        }
                        None => {
                            info!("Processed channel closed.");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Exiting cloud-sink task.");
    });

    cloud_source_task.await?;
    edge_source_task.await?;
    cloud_process_task.await?;
    edge_process_task.await?;
    cloud_sink_task.await?;
    edge_sink_task.await?;

    Ok(())
}

fn spawn_processor(
    name: &str,
    mut source_rx: mpsc::Receiver<String>,
    processed_tx: mpsc::Sender<String>,
    shutdown_token: Arc<CancellationToken>,
) -> JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        info!("Launching {name}-processor task");
        let mut seq_id = 0;

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    debug!("{name}-processor received shutdown signal");
                    break;
                }
                Some(msg) = source_rx.recv() => {
                    seq_id += 1;
                    let processed = format!("{msg} seq_id = {seq_id}");
                    debug!("Processed message from {name}: {processed}");
                    if processed_tx.send(processed).await.is_err() {
                        error!("{name} processor failed to send message");
                        break;
                    }
                }
                else => break,
            }
        }
    })
}
