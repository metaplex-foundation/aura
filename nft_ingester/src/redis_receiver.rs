use std::sync::Arc;

use async_trait::async_trait;
use entities::models::{BufferedTxWithID, UnprocessedAccountMessage};
use interface::{
    error::{MessengerError, UsecaseError},
    signature_persistence::UnprocessedTransactionsGetter,
    unprocessed_data_getter::{AccountSource, UnprocessedAccountsGetter},
};
use metrics_utils::RedisReceiverMetricsConfig;
use num_traits::Zero;
use plerkle_messenger::{
    redis_messenger::RedisMessenger, ConsumptionType, Messenger, MessengerConfig,
    TRANSACTION_STREAM,
};
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tracing::{error, info};

use crate::{error::IngesterError, message_parser::MessageParser};

pub struct RedisReceiver {
    consumption_type: ConsumptionType,
    message_parser: Arc<MessageParser>,
    messenger: Mutex<RedisMessenger>,
    ack_channel: UnboundedSender<(&'static str, String)>,
    metrics: Arc<RedisReceiverMetricsConfig>,
}

impl RedisReceiver {
    pub async fn new(
        config: MessengerConfig,
        consumption_type: ConsumptionType,
        ack_channel: UnboundedSender<(&'static str, String)>,
        metrics: Arc<RedisReceiverMetricsConfig>,
    ) -> Result<Self, IngesterError> {
        info!("Initializing RedisReceiver...");
        let message_parser = Arc::new(MessageParser::new());
        let messanger = Mutex::new(RedisMessenger::new(config).await?);
        Ok(Self { messenger: messanger, consumption_type, message_parser, ack_channel, metrics })
    }
}

#[async_trait]
impl UnprocessedTransactionsGetter for RedisReceiver {
    async fn next_transactions(&self) -> Result<Vec<BufferedTxWithID>, UsecaseError> {
        let recv_data = self
            .messenger
            .lock()
            .await
            .recv(TRANSACTION_STREAM, self.consumption_type.clone())
            .await
            .map_err(|e| UsecaseError::Messenger(MessengerError::Redis(e.to_string())))?;
        self.metrics.inc_transactions_received_by(recv_data.len() as u64);
        let mut result = Vec::new();
        for item in recv_data {
            if let Some(tx) = self.message_parser.parse_transaction(item.data, false) {
                result.push(BufferedTxWithID { tx, id: item.id })
            } else {
                self.metrics.inc_transaction_parse_errors_by(1);
            }
        }
        self.metrics.inc_transactions_parsed_by(result.len() as u64);
        Ok(result)
    }

    fn ack(&self, id: String) {
        let send = self.ack_channel.send((TRANSACTION_STREAM, id));
        if let Err(err) = send {
            error!(error = %err, "Account stream ack error: {:?}", err);
        }
    }
}

#[async_trait]
impl UnprocessedAccountsGetter for RedisReceiver {
    async fn next_accounts(
        &self,
        _batch_size: usize,
        source: &AccountSource,
    ) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError> {
        let recv_data = self
            .messenger
            .lock()
            .await
            .recv(source.to_stream_name(), self.consumption_type.clone())
            .await
            .map_err(|e| UsecaseError::Messenger(MessengerError::Redis(e.to_string())))?;
        if recv_data.is_empty() {
            tracing::debug!(
                "Stream {} is empty, returning error to wait...",
                source.to_stream_name()
            );
            return Err(UsecaseError::Messenger(MessengerError::Empty(
                source.to_stream_name().to_owned(),
            )));
        }

        self.metrics.inc_accounts_received_by(recv_data.len() as u64);
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
                    error!(error = %err, "Error parsing account: {:?}", err);
                    self.metrics.inc_account_parse_errors_by(1);
                },
            }
        }
        self.metrics
            .inc_accounts_parsed_by((result.len() + unknown_account_types_ids.len()) as u64);

        UnprocessedAccountsGetter::ack(self, unknown_account_types_ids, source);
        Ok(result)
    }

    fn ack(&self, ids: Vec<String>, source: &AccountSource) {
        for id in ids {
            if id.is_empty() {
                continue;
            }
            let send = self.ack_channel.send((source.to_stream_name(), id));
            if let Err(err) = send {
                error!(error = %err, "Account stream ack error: {:?}", err);
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
