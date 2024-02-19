use async_trait::async_trait;
use entities::models::{ClItem, ForkedItem};
use metrics_utils::red::RequestErrorDurationMetrics;
use std::collections::HashSet;
use std::sync::Arc;

#[async_trait]
pub trait ClItemsManager {
    fn items_iter(&self) -> impl Iterator<Item = ClItem>;
    async fn delete_items(
        &self,
        keys: Vec<ForkedItem>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
    );
}

#[async_trait]
pub trait ForkChecker {
    fn get_all_non_forked_slots(
        &self,
        red_metrics: Arc<RequestErrorDurationMetrics>,
    ) -> HashSet<u64>;
    fn last_slot_for_check(&self, red_metrics: Arc<RequestErrorDurationMetrics>) -> u64;
}
