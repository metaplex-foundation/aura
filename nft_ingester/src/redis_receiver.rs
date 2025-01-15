use std::sync::Arc;

use async_trait::async_trait;
use entities::models::{BufferedTxWithID, UnprocessedAccountMessage};
use interface::{
    error::UsecaseError, signature_persistence::UnprocessedTransactionsGetter,
    unprocessed_data_getter::UnprocessedAccountsGetter,
};
use num_traits::Zero;
use plerkle_messenger::{
    redis_messenger::RedisMessenger, ConsumptionType, Messenger, MessengerConfig, ACCOUNT_STREAM,
    TRANSACTION_STREAM,
};
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tracing::{info, log::error};

use crate::{error::IngesterError, message_parser::MessageParser};

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
        info!("Initializing RedisReceiver...");
        let message_parser = Arc::new(MessageParser::new());
        let messanger = Mutex::new(RedisMessenger::new(config).await?);
        Ok(Self { messanger, consumption_type, message_parser, ack_channel })
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
    async fn next_accounts(
        &self,
        _batch_size: usize,
    ) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError> {
        let recv_data = self
            .messanger
            .lock()
            .await
            .recv(ACCOUNT_STREAM, self.consumption_type.clone())
            .await
            .map_err(|e| UsecaseError::Messenger(e.to_string()))?;
        let mut result = Vec::new();
        let mut unknown_account_types_ids = Vec::new();
        for item in recv_data {
            match self.message_parser.parse_account(item.data, false) {
                Ok(accounts) => {
                    // We can receive empty UnprocessedAccountWithMetadata array if there no
                    // known account data types in item
                    // If so we need to ack item as received one but do not process it
                    if accounts.is_empty() {
                        unknown_account_types_ids.push(item.id);
                        continue;
                    }
                    for (i, account) in accounts.into_iter().enumerate() {
                        // no need to ack same element twice
                        let id = if i.is_zero() { item.id.clone() } else { String::new() };

                        result.push(UnprocessedAccountMessage {
                            account: account.unprocessed_account,
                            key: account.pubkey,
                            id,
                        });
                    }
                },
                Err(err) => {
                    error!("Parsing account: {}", err)
                },
            }
        }

        UnprocessedAccountsGetter::ack(self, unknown_account_types_ids);
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

/// id - Redis stream data ID. Which is milliseconds timestamp
/// id example - 1692632086370-0
/// where `0` is message sequence which is ignored here
pub fn get_timestamp_from_id(id: &str) -> Option<u64> {
    id.split('-').next().and_then(|t| t.parse().ok())
}
