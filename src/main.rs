mod config;
mod mqtt_client;
mod pipeline;

use log::{debug, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
