use async_trait::async_trait;
use entities::models::{ClItem, ForkedItem};

#[async_trait]
pub trait ClItemsManager {
    fn items_iter(&self) -> impl Iterator<Item = ClItem>;
    async fn delete_items(&self, keys: Vec<ForkedItem>);
}

#[async_trait]
pub trait ForkChecker {
    async fn is_forked_slot(&self, slot: u64) -> bool;
    fn last_slot_for_check(&self) -> u64;
}
