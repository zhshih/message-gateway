use log::info;

#[derive(Debug, Clone)]
pub struct CloudConfig {
    pub client_id: String,
    pub broker: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub sub_cloud_topic: String,
}

#[derive(Debug, Clone)]
pub struct EdgeConfig {
    pub broker: String,
    pub port: u16,
    pub db: u16,
    pub _password: String,
    pub sub_edge_topic: String,
}

pub fn load_config() -> (CloudConfig, EdgeConfig) {
    info!("config loaded");
    (
        CloudConfig {
            client_id: String::from("mqtt-messenger-client"),
            broker: String::from("localhost"),
            port: 1883,
            username: String::from("user"),
            password: String::from("password"),
            sub_cloud_topic: String::from("hems/into-edge/test-topic"),
        },
        EdgeConfig {
            broker: String::from("localhost"),
            port: 6379,
            db: 0,
            _password: String::from(""),
            sub_edge_topic: String::from("hems/outof-edge/test-topic"),
        },
    )
}
