use async_trait::async_trait;

#[async_trait]
pub trait MessageHandler {
    async fn call(&self, msg: Vec<u8>);
}
