use log::info;

#[derive(Debug)]
pub struct CloudConfig {
    pub client_id: String,
    pub broker: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub sub_cloud_topic: String,
    pub pub_cloud_topic: String,
}

pub fn load_config() -> CloudConfig {
    info!("config loaded");
    CloudConfig {
        client_id: String::from("mqtt-messenger-client"),
        broker: String::from("localhost"),
        port: 1883,
        username: String::from("user"),
        password: String::from("password"),
        sub_cloud_topic: String::from("cloud/edge/to-cloud"),
        pub_cloud_topic: String::from("cloud/edge/to-edge"),
    }
}
