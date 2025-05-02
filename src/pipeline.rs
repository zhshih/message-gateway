use crate::config::CloudConfig;
use crate::mqtt_client::MqttClient;
use anyhow;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub async fn start_pipeline(
    cfg: CloudConfig,
    shutdown_token: Arc<CancellationToken>,
) -> anyhow::Result<()> {
    let (cloud_client, eventloop, mut source_rx) = MqttClient::new(cfg);
    let (processed_tx, mut processed_rx) = mpsc::channel::<String>(32);

    info!("Start source job");
    cloud_client.subscribe().await?;

    let shutdown_token_clone = shutdown_token.clone();
    let source_task = tokio::spawn(MqttClient::handle_events(
        eventloop,
        cloud_client.event_sender(),
        shutdown_token_clone,
    ));

    let shutdown_token_clone = shutdown_token.clone();
    let process_task = tokio::spawn(async move {
        info!("Launching process task");
        let mut seq_id = 0;

        loop {
            tokio::select! {
                _ = shutdown_token_clone.cancelled() => {
                    debug!("Process task received shutdown signal.");
                    break;
                }
                maybe_msg = source_rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            debug!("Received source message: {msg}");
                            seq_id += 1;
                            let processed_msg = format!("{} seq_id = {}", msg, seq_id);

                            debug!("Sending processed message: {processed_msg}");
                            if let Err(e) = processed_tx.send(processed_msg.clone()).await {
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

        debug!("Process task exiting.");
    });

    let shutdown_token_clone = shutdown_token.clone();
    let sink_task = tokio::spawn(async move {
        info!("Launching sink task");

        loop {
            tokio::select! {
                _ = shutdown_token_clone.cancelled() => {
                    debug!("Sink task received shutdown signal.");
                    break;
                }
                maybe_msg = processed_rx.recv() => {
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

        debug!("Sink task exiting.");
    });

    source_task.await?;
    process_task.await?;
    sink_task.await?;

    Ok(())
}
