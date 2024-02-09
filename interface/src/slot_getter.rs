use crate::error::UsecaseError;
use async_trait::async_trait;

#[async_trait]
pub trait FinalizedSlotGetter {
    async fn get_finalized_slot(&self) -> Result<u64, UsecaseError>;
}

#[async_trait]
pub trait LastProcessedSlotGetter {
    async fn get_last_ingested_slot(&self) -> Result<Option<u64>, String>;
}
