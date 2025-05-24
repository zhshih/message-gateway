use crate::config::MessengerConfig;
use crate::messenger::cloud_client::{CloudClient, CloudEventLoop, create_cloud_client};
use crate::messenger::mqtt_client::MqttClient as MqttClientV3;
use crate::messenger::mqtt_client_v5::MqttClient as MqttClientV5;
use crate::messenger::redis_client::RedisClient;
use crate::messenger::sink::SinkPublisher;
use anyhow;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub async fn start_pipeline(
    config: MessengerConfig,
    shutdown_token: Arc<CancellationToken>,
) -> anyhow::Result<()> {
    let cloud_cfg = config.cloud_cfg;
    let edge_cfg = config.edge_cfg;
    let (cloud_client_raw, cloud_eventloop, cloud_source_rx) =
        create_cloud_client(cloud_cfg.clone());
    let (edge_client, edge_source_rx) = RedisClient::new(edge_cfg.clone()).await?;
    let cloud_client = Arc::new(cloud_client_raw);

    let shared_edge_client = Arc::new(edge_client.clone());
    shared_edge_client.spawn_heartbeat(shutdown_token.clone());

    info!("Start source task");
    match &*cloud_client {
        CloudClient::V3(c) => c.subscribe(cloud_cfg.sub_topics.clone()).await?,
        CloudClient::V5(c) => c.subscribe(cloud_cfg.sub_topics.clone()).await?,
    }

    edge_client
        .subscribe(edge_cfg.clone().sub_topics.clone())
        .await?;

    let cloud_source_task = spawn_cloud_source(
        &*cloud_client,
        cloud_eventloop,
        cloud_cfg.sub_topics.clone(),
        shutdown_token.clone(),
    );

    let shared_edge_client = Arc::new(edge_client.clone());
    let edge_source_task = tokio::spawn({
        let shutdown_token = shutdown_token.clone();
        async move {
            shared_edge_client
                .start_handling_messages(shutdown_token)
                .await;
        }
    });

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
        "cloud-sink",
        cloud_processed_rx,
        Arc::new(edge_client),
        shutdown_token.clone(),
    );

    let edge_sink_task = spawn_sink(
        "edge-sink",
        edge_processed_rx,
        cloud_client.clone(),
        shutdown_token.clone(),
    );

    cloud_source_task.await?;
    edge_source_task.await?;
    cloud_process_task.await?;
    edge_process_task.await?;
    cloud_sink_task.await?;
    edge_sink_task.await?;

    Ok(())
}

fn spawn_cloud_source(
    cloud_client: &CloudClient,
    cloud_eventloop: CloudEventLoop,
    topics: Vec<String>,
    shutdown_token: Arc<CancellationToken>,
) -> JoinHandle<()> {
    match (&*cloud_client, cloud_eventloop) {
        (CloudClient::V3(c), CloudEventLoop::V3(eventloop)) => {
            let client = c.client().clone();
            let event_tx = c.event_sender();
            let topics = topics;
            tokio::spawn(async move {
                MqttClientV3::handle_events(client, eventloop, event_tx, topics, shutdown_token)
                    .await
            })
        }
        (CloudClient::V5(c), CloudEventLoop::V5(eventloop)) => {
            let client = c.client().clone();
            let event_tx = c.event_sender();
            let topics = topics;
            tokio::spawn(async move {
                MqttClientV5::handle_events(client, eventloop, event_tx, topics, shutdown_token)
                    .await
            })
        }
        _ => panic!("Mismatched cloud client and event loop version"),
    }
}

fn spawn_processor(
    name: &str,
    mut source_rx: mpsc::Receiver<(String, String)>,
    processed_tx: mpsc::Sender<(String, String)>,
    shutdown_token: Arc<CancellationToken>,
) -> JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        info!("Launching {name}-processor task");

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
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
    name: &'static str,
    mut rx: tokio::sync::mpsc::Receiver<(String, String)>,
    publisher: Arc<P>,
    shutdown_token: Arc<CancellationToken>,
) -> tokio::task::JoinHandle<()>
where
    P: SinkPublisher,
{
    tokio::spawn(async move {
        info!("Launching {name} task");

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    debug!("{name} task received shutdown signal");
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
                            info!("{name} channel closed");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Exiting {name} task");
    })
}
