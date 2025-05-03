use crate::config::{CloudConfig, EdgeConfig};
use crate::mqtt_client::MqttClient;
use crate::redis_client::RedisClient;
use anyhow;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub async fn start_pipeline(
    cloud_cfg: CloudConfig,
    edge_cfg: EdgeConfig,
    shutdown_token: Arc<CancellationToken>,
) -> anyhow::Result<()> {
    let cloud_cfg_clone = cloud_cfg.clone();
    let (cloud_client, eventloop, mut cloud_source_rx) = MqttClient::new(cloud_cfg_clone);

    let edge_cfg_clone = edge_cfg.clone();
    let (edge_client, mut edge_source_rx) = RedisClient::new(edge_cfg_clone).await?;

    let cloud_cfg_clone = cloud_cfg.clone();
    let cloud_sub_topics = vec![cloud_cfg_clone.sub_cloud_topic.clone()];

    info!("Start source job");
    cloud_client.subscribe(cloud_sub_topics).await?;

    let edge_cfg_clone = edge_cfg.clone();
    let edge_sub_topics = vec![edge_cfg_clone.sub_edge_topic.clone()];
    let pubsub_conn = edge_client.subscribe(edge_sub_topics).await?;

    let cloud_sub_topics = vec![cloud_cfg_clone.sub_cloud_topic.clone()];
    let shutdown_token_clone = shutdown_token.clone();
    let cloud_source_task = tokio::spawn(MqttClient::handle_events(
        cloud_client.client(),
        eventloop,
        cloud_client.event_sender(),
        cloud_sub_topics,
        shutdown_token_clone,
    ));

    let shutdown_token_clone = shutdown_token.clone();
    let edge_source_task = tokio::spawn(RedisClient::handle_messages(
        pubsub_conn,
        edge_client.message_sender(),
        shutdown_token_clone,
    ));

    let (cloud_processed_tx, mut cloud_processed_rx) = mpsc::channel::<String>(32);
    let shutdown_token_clone = shutdown_token.clone();
    let cloud_process_task = tokio::spawn(async move {
        info!("Launching cloud-process task");
        let mut seq_id = 0;

        loop {
            tokio::select! {
                _ = shutdown_token_clone.cancelled() => {
                    debug!("cloud-process task received shutdown signal.");
                    break;
                }
                maybe_msg = cloud_source_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            debug!("Received source message: {msg}");
                            seq_id += 1;
                            let processed_msg = format!("{} seq_id = {}", msg, seq_id);

                            debug!("Sending processed message: {processed_msg}");
                            if let Err(e) = cloud_processed_tx.send(processed_msg.clone()).await {
                                error!("Failed to send processed message: {:?}", e);
                            } else {
                                debug!("Sent processed message: {processed_msg}");
                            }
                        }
                        None => {
                            info!("Source channel closed.");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Exiting cloud-process task.");
    });

    let (edge_processed_tx, mut edge_processed_rx) = mpsc::channel::<String>(32);
    let shutdown_token_clone = shutdown_token.clone();
    let edge_process_task = tokio::spawn(async move {
        info!("Launching edge-process task");
        let mut seq_id = 0;

        loop {
            tokio::select! {
                _ = shutdown_token_clone.cancelled() => {
                    debug!("edge-process task received shutdown signal.");
                    break;
                }
                maybe_msg = edge_source_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            debug!("Received source message: {msg}");
                            seq_id += 1;
                            let processed_msg = format!("{} seq_id = {}", msg, seq_id);

                            debug!("Sending processed message: {processed_msg}");
                            if let Err(e) = edge_processed_tx.send(processed_msg.clone()).await {
                                error!("Failed to send processed message: {:?}", e);
                            } else {
                                debug!("Sent processed message: {processed_msg}");
                            }
                        }
                        None => {
                            info!("Source channel closed.");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Exiting edge-process task.");
    });

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
