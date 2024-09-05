use crate::error::IngesterError;
use interface::message_handler::MessageHandler;
use plerkle_messenger::redis_messenger::RedisMessenger;
use plerkle_messenger::{
    ConsumptionType, Messenger, MessengerConfig, ACCOUNT_STREAM, TRANSACTION_STREAM,
};
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::log::error;

pub struct RedisReceiver<M: MessageHandler> {
    config: MessengerConfig,
    consumption_type: ConsumptionType,
    message_handler: Arc<M>,
}

impl<M: MessageHandler> RedisReceiver<M> {
    pub fn new(
        config: MessengerConfig,
        consumption_type: ConsumptionType,
        message_handler: Arc<M>,
    ) -> Self {
        Self {
            config,
            consumption_type,
            message_handler,
        }
    }

    pub async fn receive_transactions(&self, rx: Receiver<()>) -> Result<(), IngesterError> {
        let mut msg = RedisMessenger::new(self.config.clone()).await?;
        while rx.is_empty() {
            let e = msg
                .recv(TRANSACTION_STREAM, self.consumption_type.clone())
                .await;
            match e {
                Ok(_data) => {
                    // for item in data {
                    //     self.message_handler
                    //         .recv(item.data, MessageDataType::Transaction)
                    //         .await;
                    // }
                }
                Err(e) => {
                    error!("Error receiving from txn stream: {}", e);
                }
            }
        }
        Ok(())
    }

    pub async fn receive_accounts(&self, rx: Receiver<()>) -> Result<(), IngesterError> {
        let mut msg = RedisMessenger::new(self.config.clone()).await?;
        while rx.is_empty() {
            let e = msg
                .recv(ACCOUNT_STREAM, self.consumption_type.clone())
                .await;
            match e {
                Ok(_data) => {
                    // for item in data {
                    //     self.message_handler
                    //         .recv(item.data, MessageDataType::Account)
                    //         .await;
                    // }
                }
                Err(e) => {
                    error!("Error receiving from account stream: {}", e);
                }
            }
        }
        Ok(())
    }
}
