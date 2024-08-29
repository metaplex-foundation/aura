use entities::models::{ClItem, ForkedItem};
use interface::fork_cleaner::{CompressedTreeChangesManager, ForkChecker};
use metrics_utils::ForkCleanerMetricsConfig;
use rocks_db::tree_seq::TreeSeqIdxAllData;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::info;

const CI_ITEMS_DELETE_BATCH_SIZE: usize = 1000;
const SLOT_CHECK_OFFSET: u64 = 1500;

// 9000 slots == approximately one hour
const MAX_DELAY_FOR_UNKNOWN_SLOT: u64 = 9000;

pub struct ForkCleaner<CM, TM, FC>
where
    CM: CompressedTreeChangesManager<ClItem>,
    TM: CompressedTreeChangesManager<TreeSeqIdxAllData>,
    FC: ForkChecker,
{
    cl_items_manager: Arc<CM>,
    tree_seq_idx_manager: Arc<TM>,
    fork_checker: Arc<FC>,
    metrics: Arc<ForkCleanerMetricsConfig>,
}

impl<CM, TM, FC> ForkCleaner<CM, TM, FC>
where
    CM: CompressedTreeChangesManager<ClItem>,
    TM: CompressedTreeChangesManager<TreeSeqIdxAllData>,
    FC: ForkChecker,
{
    pub fn new(
        cl_items_manager: Arc<CM>,
        tree_seq_idx_manager: Arc<TM>,
        fork_checker: Arc<FC>,
        metrics: Arc<ForkCleanerMetricsConfig>,
    ) -> Self {
        Self {
            cl_items_manager,
            tree_seq_idx_manager,
            fork_checker,
            metrics,
        }
    }

    pub async fn clean_forks(&self, rx: Receiver<()>) {
        let last_slot_for_check = self
            .fork_checker
            .last_slot_for_check()
            .saturating_sub(SLOT_CHECK_OFFSET);
        let all_non_forked_slots = self.fork_checker.get_all_non_forked_slots(rx.resubscribe());
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
                self.delete_cl_items(&mut delete_items).await;
            }
        }
        if !delete_items.is_empty() {
            self.delete_cl_items(&mut delete_items).await;
        }

        for tree_seq in self.tree_seq_idx_manager.items_iter() {
            if !rx.is_empty() {
                info!("Stop iteration over tree sequence idx iterator...");
                return;
            }
            if tree_seq.slot > last_slot_for_check {
                continue;
            }

            if !all_non_forked_slots.contains(&tree_seq.slot)
                && last_slot_for_check - tree_seq.slot > MAX_DELAY_FOR_UNKNOWN_SLOT
            {
                delete_items.push(ForkedItem {
                    tree: tree_seq.tree,
                    seq: tree_seq.seq,
                    node_idx: 0, // doesn't matter here because we will drop values by tree and seq
                });
            }
            if delete_items.len() >= CI_ITEMS_DELETE_BATCH_SIZE {
                self.delete_tree_seq_idx_items(&mut delete_items).await;
            }
        }

        if !delete_items.is_empty() {
            self.delete_tree_seq_idx_items(&mut delete_items).await;
        }
        self.metrics.set_forks_detected(forked_slots.len() as i64);
    }

    async fn delete_cl_items(&self, delete_items: &mut Vec<ForkedItem>) {
        self.metrics.inc_by_deleted_items(delete_items.len() as u64);
        self.cl_items_manager
            .delete_items(std::mem::take(delete_items))
            .await;
    }

    async fn delete_tree_seq_idx_items(&self, delete_items: &mut Vec<ForkedItem>) {
        self.metrics.inc_by_deleted_items(delete_items.len() as u64);
        self.tree_seq_idx_manager
            .delete_items(std::mem::take(delete_items))
            .await;
    }
}
