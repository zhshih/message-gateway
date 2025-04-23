use crate::config::CloudConfig;
use log::{debug, error, info};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use std::time::Duration;
use tokio::sync::mpsc;

pub struct MqttClient {
    cfg: CloudConfig,
    client: AsyncClient,
    event_tx: mpsc::Sender<String>,
}

impl MqttClient {
    pub fn new(cfg: CloudConfig) -> (Self, EventLoop, mpsc::Receiver<String>) {
        let client_id = cfg.client_id.clone();
        let broker = cfg.broker.clone();
        let port = cfg.port.clone();
        let mut mqtt_opts = MqttOptions::new(&client_id, &broker, port);
        mqtt_opts.set_keep_alive(Duration::from_secs(60));
        mqtt_opts.set_credentials(cfg.username.clone(), cfg.password.clone());

        let (client, eventloop) = AsyncClient::new(mqtt_opts, 10);
        let (tx, rx) = mpsc::channel(32);

        let service = MqttClient {
            cfg,
            client,
            event_tx: tx,
        };
        (service, eventloop, rx)
    }

    pub fn event_sender(&self) -> mpsc::Sender<String> {
        self.event_tx.clone()
    }

    pub async fn subscribe(&self) {
        let topic = self.cfg.sub_cloud_topic.clone();
        info!("Subscribing to {} from cloud", topic);
        self.client
            .subscribe(&topic, QoS::AtMostOnce)
            .await
            .unwrap();
        info!("Subscribed to {} from cloud", topic);
    }

    pub async fn publish(&self, payload: &str) {
        let topic = self.cfg.pub_cloud_topic.clone();
        info!("Publishing to {} to {}", payload, topic);
        self.client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .await
            .unwrap();
        info!("Published to {} to {}", payload, topic);
    }

    pub async fn handle_events(mut eventloop: EventLoop, event_tx: mpsc::Sender<String>) {
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    debug!("Event = {notification:?}");
                    match notification {
                        rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) => {
                            let payload = String::from_utf8(p.payload.to_vec()).unwrap();
                            event_tx.send(payload).await.unwrap();
                        }
                        _ => {}
                    }
                }
                Err(err) => {
                    error!("Error = {err:?}");
                    break;
                }
            }
        }
    }
}
