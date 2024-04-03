use async_trait::async_trait;
use entities::models::{ClItem, ForkedItem};
use std::collections::HashSet;
use tokio::sync::broadcast::Receiver;

#[async_trait]
pub trait ClItemsManager {
    fn items_iter(&self) -> impl Iterator<Item = ClItem>;
    async fn delete_items(&self, keys: Vec<ForkedItem>);
}

#[async_trait]
pub trait ForkChecker {
    fn get_all_non_forked_slots(&self, rx: Receiver<()>) -> HashSet<u64>;
    fn last_slot_for_check(&self) -> u64;
}
