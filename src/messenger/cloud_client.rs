use crate::config::CloudConfig;
use crate::config::MqttVersion;
use crate::messenger::mqtt_client as mqtt_client_v3;
use crate::messenger::mqtt_client_v5;
use std::sync::Arc;
use tokio::sync::mpsc;

pub enum CloudClient {
    V3(Arc<mqtt_client_v3::MqttClient>),
    V5(Arc<mqtt_client_v5::MqttClient>),
}

pub enum CloudEventLoop {
    V3(Box<rumqttc::EventLoop>),
    V5(Box<rumqttc::v5::EventLoop>),
}

pub fn create_cloud_client(
    cfg: CloudConfig,
) -> (
    CloudClient,
    CloudEventLoop,
    mpsc::Receiver<(String, String)>,
) {
    match cfg.version {
        MqttVersion::V3 => {
            let (client, eventloop, rx) = mqtt_client_v3::MqttClient::new(cfg);
            (
                CloudClient::V3(Arc::new(client)),
                CloudEventLoop::V3(Box::new(eventloop)),
                rx,
            )
        }
        MqttVersion::V5 => {
            let (client, eventloop, rx) = mqtt_client_v5::MqttClient::new(cfg);
            (
                CloudClient::V5(Arc::new(client)),
                CloudEventLoop::V5(Box::new(eventloop)),
                rx,
            )
        }
    }
}
