use async_trait::async_trait;
use entities::models::UnprocessedAccountMessage;

use crate::error::UsecaseError;

#[derive(Debug)]
pub enum AccountSource {
    Stream,
    Backfill,
}

impl AccountSource {
    pub fn to_stream_name(&self) -> &'static str {
        match self {
            Self::Stream => plerkle_messenger::ACCOUNT_STREAM,
            Self::Backfill => plerkle_messenger::ACCOUNT_BACKFILL_STREAM,
        }
    }
}

#[async_trait]
pub trait UnprocessedAccountsGetter {
    /// FYI `batch_size` is not used for `RedisReceiver` implementation.
    async fn next_accounts(
        &self,
        batch_size: usize,
        account_source: &AccountSource,
    ) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError>;
    fn ack(&self, ids: Vec<String>, account_source: &AccountSource);
}
