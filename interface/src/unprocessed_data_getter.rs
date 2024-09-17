use crate::error::UsecaseError;
use async_trait::async_trait;
use entities::models::UnprocessedAccountMessage;

#[async_trait]
pub trait UnprocessedAccountsGetter {
    async fn next_accounts(
        &self,
        batch_size: usize,
    ) -> Result<Vec<UnprocessedAccountMessage>, UsecaseError>;
    fn ack(&self, ids: Vec<String>);
}
