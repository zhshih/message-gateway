use anyhow;
use log::info;
use serde::Deserialize;
use serde::de::{self, Deserializer};
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
    pub protocol: TransportProtocol,
    pub port: u16,
    pub sub_cloud_topics: Vec<String>,
    pub auth: AuthConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub enum MqttVersion {
    V3,
    V5,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransportProtocol {
    Tcp,
    Ws,
}

impl Default for TransportProtocol {
    fn default() -> Self {
        TransportProtocol::Tcp
    }
}

impl<'de> Deserialize<'de> for TransportProtocol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s.as_deref().unwrap_or("tcp") {
            "" | "tcp" => Ok(TransportProtocol::Tcp),
            "ws" => Ok(TransportProtocol::Ws),
            other => Err(de::Error::unknown_variant(other, &["tcp", "ws"])),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct AuthConfig {
    pub basic_enabled: bool,
    pub mtls_enabled: bool,
    pub basic: Option<BasicAuth>,
    pub mtls: Option<MtlsConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct MtlsConfig {
    pub ca_file: String,
    pub cert_file: String,
    pub key_file: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct EdgeConfig {
    pub broker: String,
    pub port: u16,
    pub db: u16,
    pub sub_edge_topics: Vec<String>,
    pub auth: AuthConfig,
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
