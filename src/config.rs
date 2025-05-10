use anyhow;
use log::info;
use serde::Deserialize;
use std::path::Path;
use tokio::fs;

#[derive(Deserialize, Debug, Clone)]
pub struct MessengerConfig {
    pub cloud_cfg: CloudConfig,
    pub edge_cfg: EdgeConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CloudConfig {
    pub client_id: String,
    pub version: MqttVersion,
    pub broker: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub sub_cloud_topics: Vec<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct EdgeConfig {
    pub broker: String,
    pub port: u16,
    pub db: u16,
    pub _password: String,
    pub sub_edge_topics: Vec<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub enum MqttVersion {
    V3,
    V5,
}

pub async fn load_config(cfg_path: &Path) -> anyhow::Result<MessengerConfig> {
    let file = fs::read(cfg_path)
        .await
        .expect("could not read configuration file")
        .iter()
        .map(|c| *c as char)
        .collect::<String>();

    let cfg: MessengerConfig = toml::from_str(&file).expect("invalid format in configuration file");

    info!("config loaded");
    anyhow::Ok(cfg)
}
