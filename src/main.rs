mod config;
mod mqtt_client;
mod pipeline;
mod redis_client;

use anyhow;
use log::{debug, error, info};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    debug!("1");

    let (cloud_conf, edge_conf) = config::load_config();
    info!("Loaded conf = ({:?}, {:?}", cloud_conf, edge_conf);

    let shutdown_token = Arc::new(CancellationToken::new());

    let shutdown_token_clone = shutdown_token.clone();
    let pipeline_handle = tokio::spawn(async move {
        if let Err(e) = pipeline::start_pipeline(cloud_conf, edge_conf, shutdown_token_clone).await
        {
            error!("Pipeline error: {:?}", e);
        }
    });

    debug!("2");

    tokio::signal::ctrl_c().await?;
    info!("Starting shutdown...");
    shutdown_token.cancel();

    pipeline_handle.await?;

    info!("Completed Shutdown");
    Ok(())
}
