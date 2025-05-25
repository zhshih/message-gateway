mod config;
mod messenger;
mod pipeline;

use anyhow;
use log::{error, info};
use rustls;
use std::env;
use std::path::Path;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

const CONF_FILENAME: &str = "config.toml";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    info!("*** Installing default CryptoProvider ***");
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    info!("*** Launching main service ***");

    let cfg_path_str = env::var("CONFIG_PATH").unwrap_or_else(|_| CONF_FILENAME.to_string());
    let cfg_path = Path::new(&cfg_path_str);
    info!("loading config from file {:?}", cfg_path);
    let result = config::load_config(cfg_path).await;
    let messenger_cfg = match result {
        Ok(messenger_cfg) => {
            info!("Loaded conf = ({:?}", messenger_cfg);
            messenger_cfg
        }
        Err(err) => {
            error!("Event loop error: {err:?}");
            std::process::exit(-1);
        }
    };

    let shutdown_token = Arc::new(CancellationToken::new());

    let shutdown_token_clone = shutdown_token.clone();
    let pipeline_handle = tokio::spawn(async move {
        if let Err(e) = pipeline::start_pipeline(messenger_cfg, shutdown_token_clone).await {
            error!("Pipeline error: {:?}", e);
        }
    });

    info!("*** Launched main service ***");

    tokio::signal::ctrl_c().await?;
    info!("Starting shutdown...");
    shutdown_token.cancel();

    pipeline_handle.await?;

    info!("Completed Shutdown");
    Ok(())
}
