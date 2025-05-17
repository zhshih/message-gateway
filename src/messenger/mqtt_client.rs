use crate::config::{CloudConfig, TransportProtocol};
use crate::messenger::sink::SinkPublisher;
use anyhow;
use log::{debug, error, info};
use rumqttc::{
    AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, TlsConfiguration, Transport,
};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct MqttClient {
    client: AsyncClient,
    event_tx: mpsc::Sender<(String, String)>,
}

impl MqttClient {
    pub fn new(cfg: CloudConfig) -> (Self, EventLoop, mpsc::Receiver<(String, String)>) {
        let mqtt_opts = Self::build_mqtt_options(&cfg);

        let (client, eventloop) = AsyncClient::new(mqtt_opts, 10);
        let (tx, rx) = mpsc::channel::<(String, String)>(32);

        let service = MqttClient {
            client,
            event_tx: tx,
        };
        (service, eventloop, rx)
    }

    pub fn event_sender(&self) -> mpsc::Sender<(String, String)> {
        self.event_tx.clone()
    }

    pub fn client(&self) -> AsyncClient {
        return self.client.clone();
    }

    pub async fn subscribe(&self, topics: Vec<String>) -> anyhow::Result<()> {
        for topic in topics {
            debug!("Subscribing to {} from cloud", topic);

            self.client
                .subscribe(&topic, QoS::AtMostOnce)
                .await
                .map_err(|e| {
                    error!("Failed to subscribe to topic: {e}");
                    anyhow::anyhow!(e)
                })?;

            info!("Subscribed to {} from cloud", topic);
        }
        Ok(())
    }

    pub async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        debug!("Publishing to {} to {}", payload, topic);

        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await
            .map_err(|e| {
                error!("Failed to publish to topic: {e}");
                anyhow::anyhow!(e)
            })?;

        info!("Published to {} to {}", payload, topic);
        Ok(())
    }

    pub async fn handle_events(
        client: AsyncClient,
        mut eventloop: EventLoop,
        event_tx: mpsc::Sender<(String, String)>,
        topics: Vec<String>,
        shutdown_token: Arc<CancellationToken>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    debug!("Shutdown signal received");
                    break;
                }
                result = eventloop.poll() => {
                    match result {
                        Ok(event) => {
                            debug!("Event received: {event:?}");

                            match event {
                                Event::Incoming(Packet::Publish(p)) => {
                                    match String::from_utf8(p.payload.to_vec()) {
                                        Ok(payload) => {
                                            debug!("Received payload: {}, from {}", payload, p.topic);
                                            if let Err(e) = event_tx.send((p.topic, payload)).await {
                                                error!("Failed to send event: {e}");
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to parse payload as UTF-8: {e}");
                                        }
                                    }
                                }
                                Event::Incoming(Packet::ConnAck(_)) => {
                                    for topic in &topics {
                                        if let Err(e) = client.subscribe(topic, QoS::AtMostOnce).await {
                                            error!("Failed to re-subscribe to topic {topic}: {e}");
                                        } else {
                                            info!("Re-subscribed to topic {topic}");
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        Err(err) => {
                            error!("Event loop error: {err:?}");
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                }
            }
        }
    }

    fn build_mqtt_options(cfg: &CloudConfig) -> MqttOptions {
        let mut broker = cfg.broker.clone();
        if cfg.protocol == TransportProtocol::Ws {
            let protocol = if cfg.auth.mtls_enabled { "wss" } else { "ws" };
            broker = format!("{}://{}:{}/mqtt", protocol, broker, cfg.port);
        }
        let mut mqtt_opts = MqttOptions::new(&cfg.client_id, &broker, cfg.port);
        mqtt_opts.set_keep_alive(Duration::from_secs(60));
        Self::configure_auth(cfg, &mut mqtt_opts).expect("Configure auth failed");

        mqtt_opts
    }

    fn configure_auth(
        cfg: &CloudConfig,
        mqtt_opts: &mut MqttOptions,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tls_config = if cfg.auth.mtls_enabled {
            let mtls_cfg = cfg.auth.mtls.as_ref().ok_or("mTLS config missing")?;
            let ca = fs::read(&mtls_cfg.ca_file)?;
            let client_cert = fs::read(&mtls_cfg.cert_file)?;
            let client_key = fs::read(&mtls_cfg.key_file)?;
            Some(TlsConfiguration::Simple {
                ca,
                client_auth: Some((client_cert, client_key)),
                alpn: None,
            })
        } else {
            None
        };

        match (&cfg.protocol, cfg.auth.basic_enabled, cfg.auth.mtls_enabled) {
            (TransportProtocol::Ws, true, true) => {
                info!("MQTT over Websocket with basic auth and mTLS enabled");
                mqtt_opts.set_credentials(
                    cfg.auth.basic.as_ref().unwrap().username.clone(),
                    cfg.auth.basic.as_ref().unwrap().password.clone(),
                );
                let tls_config = tls_config.ok_or("TLS config required for WSS")?;
                mqtt_opts.set_transport(Transport::Wss(tls_config));
            }
            (TransportProtocol::Ws, true, false) => {
                info!("MQTT over Websocket with basic auth and no-mTLS enabled");
                mqtt_opts.set_credentials(
                    cfg.auth.basic.as_ref().unwrap().username.clone(),
                    cfg.auth.basic.as_ref().unwrap().password.clone(),
                );
                mqtt_opts.set_transport(Transport::Ws);
            }
            (TransportProtocol::Ws, false, true) => {
                info!("MQTT over Websocket with no-basic auth and mTLS enabled");
                let tls_config = tls_config.ok_or("TLS config required for TLS")?;
                mqtt_opts.set_transport(Transport::Wss(tls_config));
            }
            (TransportProtocol::Ws, false, false) => {
                info!("MQTT over Websocket with no-basic auth and no-mTLS enabled");
                mqtt_opts.set_transport(Transport::Ws);
            }
            (TransportProtocol::Tcp, true, true) => {
                info!("MQTT over TCP with basic auth and mTLS enabled");
                mqtt_opts.set_credentials(
                    cfg.auth.basic.as_ref().unwrap().username.clone(),
                    cfg.auth.basic.as_ref().unwrap().password.clone(),
                );
                let tls_config = tls_config.ok_or("TLS config required for TLS")?;
                mqtt_opts.set_transport(Transport::Tls(tls_config));
            }
            (TransportProtocol::Tcp, true, false) => {
                info!("MQTT over TCP with basic auth and no-mTLS enabled");
                mqtt_opts.set_credentials(
                    cfg.auth.basic.as_ref().unwrap().username.clone(),
                    cfg.auth.basic.as_ref().unwrap().password.clone(),
                );
                mqtt_opts.set_transport(Transport::Tcp);
            }
            (TransportProtocol::Tcp, false, true) => {
                info!("MQTT over TCP with no-basic auth and mTLS enabled");
                let tls_config = tls_config.ok_or("TLS config required for TLS")?;
                mqtt_opts.set_transport(Transport::Tls(tls_config));
            }
            _ => {
                info!("MQTT over TCP with no-basic auth and no-mTLS enabled");
                mqtt_opts.set_transport(Transport::Tcp);
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl SinkPublisher for MqttClient {
    async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        self.publish(topic, payload).await
    }
}
