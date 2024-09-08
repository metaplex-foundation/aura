use crate::error::IngesterError;
use crate::message_parser::MessageParser;
use async_trait::async_trait;
use entities::models::{BufferedTxWithID, UnprocessedAccountMessage};
use interface::error::UsecaseError;
use interface::signature_persistence::UnprocessedTransactionsGetter;
use interface::unprocessed_data_getter::UnprocessedAccountsGetter;
use num_traits::Zero;
use plerkle_messenger::redis_messenger::RedisMessenger;
use plerkle_messenger::{
    ConsumptionType, Messenger, MessengerConfig, ACCOUNT_STREAM, TRANSACTION_STREAM,
};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tracing::log::error;

pub struct RedisReceiver {
    consumption_type: ConsumptionType,
    message_parser: Arc<MessageParser>,
    messanger: Mutex<RedisMessenger>,
    ack_channel: UnboundedSender<(&'static str, String)>,
}

impl RedisReceiver {
    pub async fn new(
        config: MessengerConfig,
        consumption_type: ConsumptionType,
        ack_channel: UnboundedSender<(&'static str, String)>,
    ) -> Result<Self, IngesterError> {
        let message_parser = Arc::new(MessageParser::new());
        let messanger = Mutex::new(RedisMessenger::new(config).await?);
        Ok(Self {
            messanger,
            consumption_type,
            message_parser,
            ack_channel,
        })
    }
}

#[async_trait]
impl UnprocessedTransactionsGetter for RedisReceiver {
    async fn next_transactions(&self) -> Result<Vec<BufferedTxWithID>, UsecaseError> {
        let recv_data = self
            .messanger
            .lock()
            .await
            .recv(TRANSACTION_STREAM, self.consumption_type.clone())
            .await
            .map_err(|e| UsecaseError::Messenger(e.to_string()))?;
        let mut result = Vec::new();
        for item in recv_data {
            if let Some(tx) = self.message_parser.parse_transaction(item.data, false) {
                result.push(BufferedTxWithID { tx, id: item.id })
            }
        }
        Ok(result)
    }

    fn ack(&self, id: String) {
        let send = self.ack_channel.send((TRANSACTION_STREAM, id));
        if let Err(err) = send {
            error!("Account stream ack error: {}", err);
        }
    }
}

#[async_trait]
impl UnprocessedAccountsGetter for RedisReceiver {
    async fn next_accounts(&self) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError> {
        let recv_data = self
            .messanger
            .lock()
            .await
            .recv(ACCOUNT_STREAM, self.consumption_type.clone())
            .await
            .map_err(|e| UsecaseError::Messenger(e.to_string()))?;
        let mut result = Vec::new();
        for item in recv_data {
            match self.message_parser.parse_account(item.data, false).await {
                Ok(accounts) => {
                    for (i, account) in accounts.into_iter().enumerate() {
                        // no need to ack same element twice
                        let id = if i.is_zero() {
                            item.id.clone()
                        } else {
                            String::new()
                        };

                        result.push(UnprocessedAccountMessage {
                            account: account.unprocessed_account,
                            key: account.pubkey,
                            id,
                        });
                    }
                }
                Err(err) => {
                    error!("Parsing account: {}", err)
                }
            }
        }

        Ok(result)
    }

    fn ack(&self, ids: Vec<String>) {
        for id in ids {
            if id.is_empty() {
                continue;
            }
            let send = self.ack_channel.send((ACCOUNT_STREAM, id));
            if let Err(err) = send {
                error!("Account stream ack error: {}", err);
            }
        }
    }
}
