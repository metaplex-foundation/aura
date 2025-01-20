use async_trait::async_trait;
use entities::models::UnprocessedAccountMessage;

use crate::error::UsecaseError;

#[async_trait]
pub trait UnprocessedAccountsGetter {
    /// FYI `batch_size` is not used for `RedisReceiver` implementation.
    async fn next_accounts(
        &self,
        batch_size: usize,
    ) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError>;
    fn ack(&self, ids: Vec<String>);
}
