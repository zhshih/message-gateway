mod config;
mod mqtt_client;
mod pipeline;

use log::{debug, info};
use anyhow;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    debug!("1");

    let conf = config::load_config();
    info!("Loaded conf = {:?}", conf);

    let _ = tokio::spawn(async move {
        pipeline::start_pipeline(conf).await.unwrap();
    });

    debug!("2");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}
