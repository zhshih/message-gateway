use crate::config::CloudConfig;
use crate::mqtt_client::MqttClient;
use anyhow;
use log::{debug, error, info};
use tokio::sync::mpsc;

pub async fn start_pipeline(cfg: CloudConfig) -> anyhow::Result<()> {
    let (cloud_client, eventloop, mut source_rx) = MqttClient::new(cfg);
    let (processed_tx, mut processed_rx) = mpsc::channel::<String>(32);

    info!("Start source job");
    cloud_client.subscribe().await?;

    let source_task = tokio::spawn(MqttClient::handle_events(
        eventloop,
        cloud_client.event_sender(),
    ));

    let process_task = tokio::spawn(async move {
        info!("Launching process task");
        let mut seq_id = 0;
        while let Some(msg) = source_rx.recv().await {
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
    });

    let sink_task = tokio::spawn(async move {
        info!("Launching sink task");
        while let Some(msg) = processed_rx.recv().await {
            debug!("Received processed message: {msg}");

            debug!("Publishing sink message: {msg}");
            if let Err(e) = cloud_client.publish(&msg).await {
                error!("Failed to publish message: {:?}", e);
            } else {
                debug!("Published sink message: {msg}");
            }
        }
    });

    source_task.await?;
    process_task.await?;
    sink_task.await?;

    Ok(())
}
