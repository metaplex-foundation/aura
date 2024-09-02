use async_trait::async_trait;
use entities::enums::MessageDataType;

#[async_trait]
pub trait MessageHandler {
    async fn call(&self, msg: Vec<u8>);
    async fn recv(&self, msg: Vec<u8>, message_type: MessageDataType);
}
