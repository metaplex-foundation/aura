use entities::models::ForkedItem;
use interface::fork_cleaner::{ClItemsManager, ForkChecker};
use metrics_utils::ForkCleanerMetricsConfig;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::info;

const CI_ITEMS_DELETE_BATCH_SIZE: usize = 1000;
const SLOT_CHECK_OFFSET: u64 = 1000;

pub struct ForkCleaner<CM, FC>
where
    CM: ClItemsManager,
    FC: ForkChecker,
{
    cl_items_manager: Arc<CM>,
    fork_checker: Arc<FC>,
    metrics: Arc<ForkCleanerMetricsConfig>,
}

impl<CM, FC> ForkCleaner<CM, FC>
where
    CM: ClItemsManager,
    FC: ForkChecker,
{
    pub fn new(
        cl_items_manager: Arc<CM>,
        fork_checker: Arc<FC>,
        metrics: Arc<ForkCleanerMetricsConfig>,
    ) -> Self {
        Self {
            cl_items_manager,
            fork_checker,
            metrics,
        }
    }

    pub async fn clean_forks(&self, rx: Receiver<()>) {
        let last_slot_for_check = self
            .fork_checker
            .last_slot_for_check(self.metrics.red_metrics.clone())
            .saturating_sub(SLOT_CHECK_OFFSET);
        let all_non_forked_slots = self
            .fork_checker
            .get_all_non_forked_slots(self.metrics.red_metrics.clone());
        let mut forked_slots = HashSet::new();
        let mut delete_items = Vec::new();
        for cl_item in self.cl_items_manager.items_iter() {
            if !rx.is_empty() {
                info!("Stop iteration over cl items iterator...");
                return;
            }
            if cl_item.slot_updated > last_slot_for_check {
                continue;
            }
            if !all_non_forked_slots.contains(&cl_item.slot_updated) {
                delete_items.push(ForkedItem {
                    tree: cl_item.cli_tree_key,
                    seq: cl_item.cli_seq,
                    node_idx: cl_item.cli_node_idx,
                });
                forked_slots.insert(cl_item.slot_updated);
            }
            if delete_items.len() >= CI_ITEMS_DELETE_BATCH_SIZE {
                self.delete_items(&mut delete_items).await;
            }
        }
        if !delete_items.is_empty() {
            self.delete_items(&mut delete_items).await;
        }
        self.metrics.set_forks_detected(forked_slots.len() as i64);
    }

    async fn delete_items(&self, delete_items: &mut Vec<ForkedItem>) {
        self.metrics.inc_by_deleted_items(delete_items.len() as u64);
        self.cl_items_manager
            .delete_items(
                std::mem::take(delete_items),
                self.metrics.red_metrics.clone(),
            )
            .await;
    }
}
