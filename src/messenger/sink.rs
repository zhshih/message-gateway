use crate::messenger::cloud_client::CloudClient;

#[async_trait::async_trait]
pub trait SinkPublisher: Send + Sync + 'static {
    async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl SinkPublisher for CloudClient {
    async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()> {
        match self {
            CloudClient::V3(c) => c.publish(topic, payload).await,
            CloudClient::V5(c) => c.publish(topic, payload).await,
        }
    }
}
