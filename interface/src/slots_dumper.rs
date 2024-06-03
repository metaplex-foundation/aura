use async_trait::async_trait;

use crate::error::StorageError;

#[async_trait]
pub trait SlotsDumper {
    async fn dump_slots(&self, slots: &[u64]);
}

#[async_trait]
pub trait SlotGetter {
    fn get_unprocessed_slots_iter(&self) -> impl Iterator<Item = u64>;
    async fn mark_slots_processed(&self, slots: Vec<u64>) -> Result<(), StorageError>;
}
