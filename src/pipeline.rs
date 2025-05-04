use crate::config::{CloudConfig, EdgeConfig};
use crate::mqtt_client::MqttClient;
use crate::redis_client::RedisClient;
use crate::sink::SinkPublisher;
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
    cloud_client
        .subscribe(cloud_cfg.clone().sub_cloud_topic.clone())
        .await?;

    let pubsub_conn = edge_client
        .subscribe(edge_cfg.clone().sub_edge_topic.clone())
        .await?;

    let cloud_source_task = tokio::spawn(MqttClient::handle_events(
        cloud_client.client(),
        eventloop,
        cloud_client.event_sender(),
        cloud_cfg.clone().sub_cloud_topic.clone(),
        shutdown_token.clone(),
    ));

    let edge_source_task = tokio::spawn(RedisClient::handle_messages(
        pubsub_conn,
        edge_client.message_sender(),
        shutdown_token.clone(),
    ));

    info!("Start process task");
    let (cloud_processed_tx, cloud_processed_rx) = mpsc::channel::<(String, String)>(32);
    let cloud_process_task = spawn_processor(
        "cloud",
        cloud_source_rx,
        cloud_processed_tx,
        shutdown_token.clone(),
    );

    let (edge_processed_tx, edge_processed_rx) = mpsc::channel::<(String, String)>(32);
    let edge_process_task = spawn_processor(
        "edge",
        edge_source_rx,
        edge_processed_tx,
        shutdown_token.clone(),
    );

    info!("Start sink task");
    let cloud_sink_task = spawn_sink(
        shutdown_token.clone(),
        cloud_processed_rx,
        Arc::new(edge_client),
        "cloud-sink",
    );

    let edge_sink_task = spawn_sink(
        shutdown_token.clone(),
        edge_processed_rx,
        Arc::new(cloud_client),
        "edge-sink",
    );

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
    mut source_rx: mpsc::Receiver<(String, String)>,
    processed_tx: mpsc::Sender<(String, String)>,
    shutdown: Arc<CancellationToken>,
) -> JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        info!("Launching {name}-processor task");

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("{name}-processor received shutdown signal");
                    break;
                }
                Some((mut topic, msg)) = source_rx.recv() => {
                    if name == "cloud" {
                        if let Some(_) = topic.find("/into-edge") {
                            topic = topic.replace("/into-edge", "");
                        }
                    } else if name == "edge" {
                        if let Some(_) = topic.find("/outof-edge") {
                            topic = topic.replace("/outof-edge", "");
                        }
                    }
                    debug!("Processed message from {name}: ({topic}, {msg})");
                    if processed_tx.send((topic, msg)).await.is_err() {
                        error!("{name} processor failed to send message");
                        break;
                    }
                }
                else => break,
            }
        }
    })
}

fn spawn_sink<P>(
    shutdown_token: Arc<CancellationToken>,
    mut rx: tokio::sync::mpsc::Receiver<(String, String)>,
    publisher: Arc<P>,
    sink_name: &'static str,
) -> tokio::task::JoinHandle<()>
where
    P: SinkPublisher,
{
    tokio::spawn(async move {
        info!("Launching {sink_name} task");

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    debug!("{sink_name} task received shutdown signal");
                    break;
                }
                maybe_processed_info = rx.recv() => {
                    match maybe_processed_info {
                        Some((topic, msg)) => {
                            debug!("Publishing sink message: {msg} to {topic}");
                            if let Err(e) = publisher.publish(&topic, &msg).await {
                                error!("Failed to publish message: {:?}", e);
                            } else {
                                debug!("Published sink message");
                            }
                        }
                        None => {
                            info!("{sink_name} channel closed");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Exiting {sink_name} task");
    })
}
