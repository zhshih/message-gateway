#[async_trait::async_trait]
pub trait SinkPublisher: Send + Sync + 'static {
    async fn publish(&self, topic: &str, payload: &str) -> anyhow::Result<()>;
}
