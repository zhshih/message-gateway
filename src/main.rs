mod config;
mod mqtt_client;
mod pipeline;

use anyhow;
use log::{debug, error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    debug!("1");

    let conf = config::load_config();
    info!("Loaded conf = {:?}", conf);

    let _ = tokio::spawn(async move {
        if let Err(e) = pipeline::start_pipeline(conf).await {
            error!("Pipeline error: {:?}", e);
        }
    });

    debug!("2");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}
