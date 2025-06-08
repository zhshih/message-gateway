use crate::config::{CloudConfig, EdgeConfig, MessengerConfig};
use crate::messenger::cloud_client::{CloudClient, CloudEventLoop, create_cloud_client};
use crate::messenger::mqtt_client::MqttClient as MqttClientV3;
use crate::messenger::mqtt_client_v5::MqttClient as MqttClientV5;
use crate::messenger::redis_client::RedisClient;
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;

const DEFAULT_PIPELINE_RETRY_INTERVAL: u64 = 3;

pub async fn start_pipeline(
    config: MessengerConfig,
    shutdown_token: Arc<CancellationToken>,
) -> anyhow::Result<()> {
    loop {
        let (cloud_sink_tx, cloud_sink_rx) = mpsc::channel::<(String, String)>(32);
        let (edge_sink_tx, edge_sink_rx) = mpsc::channel::<(String, String)>(32);

        let cloud_cfg = config.clone().cloud_cfg;
        let edge_cfg = config.clone().edge_cfg;

        let edge_task = tokio::spawn(start_edge_pipeline(
            edge_cfg.clone(),
            edge_sink_tx,
            cloud_sink_rx,
            shutdown_token.clone(),
        ));

        let cloud_task = tokio::spawn(start_cloud_pipeline(
            cloud_cfg.clone(),
            cloud_sink_tx,
            edge_sink_rx,
            shutdown_token.clone(),
        ));

        match tokio::try_join!(edge_task, cloud_task) {
            Ok((Ok(_), Ok(_))) => {
                info!("Pipeline completed successfully.");
                return Ok(());
            }
            Ok((edge_res, cloud_res)) => {
                if edge_res.is_err() {
                    error!("Edge pipeline failed: {:?}", edge_res.unwrap_err());
                }
                if cloud_res.is_err() {
                    error!("Cloud pipeline failed: {:?}", cloud_res.unwrap_err());
                }
            }
            Err(join_err) => {
                error!("Join error: {:?}", join_err);
            }
        }
        warn!("Retrying pipeline in {DEFAULT_PIPELINE_RETRY_INTERVAL} seconds...)");
        sleep(Duration::from_secs(DEFAULT_PIPELINE_RETRY_INTERVAL)).await;
    }
}

async fn start_edge_pipeline(
    cfg: EdgeConfig,
    edge_sink_tx: tokio::sync::mpsc::Sender<(String, String)>,
    cloud_sink_rx: tokio::sync::mpsc::Receiver<(String, String)>,
    shutdown_token: Arc<CancellationToken>,
) -> anyhow::Result<()> {
    let (edge_client, edge_source_rx) = RedisClient::new(cfg.clone()).await?;

    let shared_edge_client = Arc::new(edge_client.clone());
    shared_edge_client.spawn_heartbeat(shutdown_token.clone());

    edge_client
        .subscribe(cfg.clone().sub_topics.clone())
        .await?;

    info!("Launching edge-source task");
    let shared_edge_client = Arc::new(edge_client.clone());
    let edge_source_task = tokio::spawn({
        let shutdown_token = shutdown_token.clone();
        async move {
            shared_edge_client
                .start_handling_messages(shutdown_token)
                .await;
        }
    });

    info!("Launching edge-bridge task");
    let shared_edge_client = Arc::new(edge_client.clone());
    shared_edge_client.start_bridging_task(cloud_sink_rx, shutdown_token.clone());

    let (edge_processed_tx, edge_processed_rx) = mpsc::channel::<(String, String)>(32);
    let edge_process_task = spawn_processor(
        "edge",
        edge_source_rx,
        edge_processed_tx,
        shutdown_token.clone(),
    );

    let edge_sink_task = spawn_sink(
        "edge-sink",
        edge_processed_rx,
        edge_sink_tx,
        shutdown_token.clone(),
    );

    edge_source_task.await?;
    edge_process_task.await?;
    edge_sink_task.await?;

    Ok(())
}

async fn start_cloud_pipeline(
    cfg: CloudConfig,
    cloud_sink_tx: tokio::sync::mpsc::Sender<(String, String)>,
    edge_sink_rx: tokio::sync::mpsc::Receiver<(String, String)>,
    shutdown_token: Arc<CancellationToken>,
) -> anyhow::Result<()> {
    let (cloud_client_raw, cloud_eventloop, cloud_source_rx) = create_cloud_client(cfg.clone());
    let cloud_client = Arc::new(cloud_client_raw);

    match &*cloud_client {
        CloudClient::V3(c) => c.subscribe(cfg.sub_topics.clone()).await?,
        CloudClient::V5(c) => c.subscribe(cfg.sub_topics.clone()).await?,
    }

    info!("Launching cloud-source task");
    let cloud_source_task = spawn_cloud_source(
        &cloud_client,
        cloud_eventloop,
        cfg.sub_topics.clone(),
        shutdown_token.clone(),
    );

    info!("Launching cloud-bridge task");
    start_bridging_task(&cloud_client, edge_sink_rx, shutdown_token.clone());

    let (cloud_processed_tx, cloud_processed_rx) = mpsc::channel::<(String, String)>(32);
    let cloud_process_task = spawn_processor(
        "cloud",
        cloud_source_rx,
        cloud_processed_tx,
        shutdown_token.clone(),
    );

    let cloud_sink_task = spawn_sink(
        "cloud-sink",
        cloud_processed_rx,
        cloud_sink_tx,
        shutdown_token.clone(),
    );

    cloud_source_task.await?;
    cloud_process_task.await?;
    cloud_sink_task.await?;

    Ok(())
}

fn spawn_cloud_source(
    cloud_client: &CloudClient,
    cloud_eventloop: CloudEventLoop,
    topics: Vec<String>,
    shutdown_token: Arc<CancellationToken>,
) -> JoinHandle<()> {
    match (&cloud_client, cloud_eventloop) {
        (CloudClient::V3(c), CloudEventLoop::V3(eventloop)) => {
            let client = c.client().clone();
            let event_tx = c.event_sender();
            tokio::spawn(async move {
                MqttClientV3::handle_events(client, *eventloop, event_tx, topics, shutdown_token)
                    .await
            })
        }
        (CloudClient::V5(c), CloudEventLoop::V5(eventloop)) => {
            let client = c.client().clone();
            let event_tx = c.event_sender();
            tokio::spawn(async move {
                MqttClientV5::handle_events(client, *eventloop, event_tx, topics, shutdown_token)
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
                    if name == "cloud" && topic.contains("/into-edge") {
                        topic = topic.replace("/into-edge", "");
                    } else if name == "edge" && topic.contains("/outof-edge") {
                        topic = topic.replace("/outof-edge", "");
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

fn spawn_sink(
    name: &'static str,
    mut rx: tokio::sync::mpsc::Receiver<(String, String)>,
    tx: tokio::sync::mpsc::Sender<(String, String)>,
    shutdown_token: Arc<CancellationToken>,
) -> tokio::task::JoinHandle<()> {
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
                            if let Err(e) = tx.send((topic, msg)).await {
                                error!("Failed to publish message: {:?}", e);
                                break;
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

fn start_bridging_task(
    cloud_client: &CloudClient,
    rx: tokio::sync::mpsc::Receiver<(String, String)>,
    shutdown_token: Arc<CancellationToken>,
) {
    match &cloud_client {
        CloudClient::V3(c) => c.clone().start_bridging_task(rx, shutdown_token),
        CloudClient::V5(c) => c.clone().start_bridging_task(rx, shutdown_token),
    }
}
