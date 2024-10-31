use entities::models::ForkedItem;
use interface::fork_cleaner::{CompressedTreeChangesManager, ForkChecker};
use metrics_utils::ForkCleanerMetricsConfig;
use rocks_db::storage_consistency::BubblegumChangeKey;
use rocks_db::storage_consistency::DataConsistencyStorage;
use rocks_db::Storage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinError;
use tokio::time::sleep as tokio_sleep;
use tokio::time::Instant;
use tracing::info;

use crate::consistency_calculator::NftChangesTracker;

const CI_ITEMS_DELETE_BATCH_SIZE: usize = 100;
const SLOT_CHECK_OFFSET: u64 = 1500;

pub async fn run_fork_cleaner(
    fork_cleaner: ForkCleaner<Storage, Storage>,
    metrics: Arc<ForkCleanerMetricsConfig>,
    mut rx: Receiver<()>,
    sequence_consistent_checker_wait_period_sec: u64,
) -> Result<(), JoinError> {
    info!("Start cleaning forks...");
    loop {
        let start = Instant::now();
        fork_cleaner.clean_forks(rx.resubscribe()).await;
        metrics.set_scans_latency(start.elapsed().as_secs_f64());
        metrics.inc_total_scans();
        tokio::select! {
            _ = tokio_sleep(Duration::from_secs(sequence_consistent_checker_wait_period_sec)) => {},
            _ = rx.recv() => {
                info!("Received stop signal, stopping cleaning forks!");
                break;
            }
        }
    }

    Ok(())
}

pub struct ForkCleaner<CM, FC>
where
    CM: CompressedTreeChangesManager,
    FC: ForkChecker,
{
    cl_items_manager: Arc<CM>,
    fork_checker: Arc<FC>,
    data_consistency_storage: Arc<dyn DataConsistencyStorage + Send + Sync>,
    nft_changes_tracker: Option<Arc<NftChangesTracker>>,
    metrics: Arc<ForkCleanerMetricsConfig>,
}

impl<CM, FC> ForkCleaner<CM, FC>
where
    CM: CompressedTreeChangesManager,
    FC: ForkChecker,
{
    pub fn new(
        cl_items_manager: Arc<CM>,
        fork_checker: Arc<FC>,
        data_consistency_storage: Arc<dyn DataConsistencyStorage + Send + Sync>,
        nft_changes_tracker: Option<Arc<NftChangesTracker>>,
        metrics: Arc<ForkCleanerMetricsConfig>,
    ) -> Self {
        Self {
            cl_items_manager,
            fork_checker,
            data_consistency_storage,
            nft_changes_tracker,
            metrics,
        }
    }

    pub async fn clean_forks(&self, rx: Receiver<()>) {
        let last_slot_for_check = self
            .fork_checker
            .last_slot_for_check()
            .saturating_sub(SLOT_CHECK_OFFSET);
        let all_non_forked_slots = self.fork_checker.get_all_non_forked_slots(rx.resubscribe());

        let mut forked_slots = 0;
        let mut delete_items = Vec::new();
        let mut changes_to_delete = Vec::new();

        let mut signatures_to_drop = Vec::new();

        // fork cleaner iterate over signatures which are saved for each parsed transaction
        // so even if transaction was in fork this column family has it
        for signature in self.cl_items_manager.tree_seq_idx_iter() {
            if let Some(max_slot) = signature.slot_sequences.keys().max() {
                // if max slot for selected transaction(tx) is greater then last_slot_for_check
                // it means that tx is fresh and we should not check it such as there is high possibility
                // that it's updates will be overwritten
                if max_slot > &last_slot_for_check {
                    continue;
                }

                // here we have a vector because forked transaction can appear in different slots with same sequence
                // in such case we have to check if one of those blocks is in fork
                let mut slots_with_highest_sequence = vec![];
                // looking for a block with highest sequence because CLItems merge function checks that value
                // meaning CLItems will contain updates from the transaction with highest sequence, even if it has the lowest slot number
                let mut highest_sequence = 0;

                for (slot, sequences) in &signature.slot_sequences {
                    for seq in sequences {
                        match seq.cmp(&highest_sequence) {
                            std::cmp::Ordering::Greater => {
                                highest_sequence = *seq;
                                slots_with_highest_sequence.clear(); // Clear previous slots since a new highest sequence is found
                                slots_with_highest_sequence.push(*slot);
                            }
                            std::cmp::Ordering::Equal => {
                                slots_with_highest_sequence.push(*slot);
                            }
                            std::cmp::Ordering::Less => {
                                // Do nothing
                            }
                        }
                    }
                }

                let mut clean_up = false;
                // check if either of slots appeared in fork
                for slot in slots_with_highest_sequence {
                    if !all_non_forked_slots.contains(&slot) {
                        clean_up = true;

                        forked_slots += 1;
                    }
                }

                if clean_up {
                    // if at least one of the blocks appeared in a fork we need to drop all the tree sequences which are related to transaction
                    // which fork cleaner is processing at the moment.
                    //
                    // since we may have saved sequence 5 (which is forked) to CLItems,
                    // but the valid sequence for this transaction on the main branch is actually 4,
                    // dropping only sequence 5 would result in an incorrect update during backfill.
                    // therefore, we need to drop sequence 4 as well. Sequence 5 must be dropped because
                    // it contains a different tree update in the main branch
                    for (slot, sequences) in signature.slot_sequences.iter() {
                        for seq in sequences {
                            delete_items.push(ForkedItem {
                                tree: signature.tree,
                                seq: *seq,
                                // in this context it doesn't matter what value we put in here
                                // because deletion will happen by tree and seq values
                                node_idx: 0,
                            });

                            changes_to_delete.push(BubblegumChangeKey::new(
                                signature.tree,
                                *slot,
                                *seq,
                            ));
                        }
                    }
                }

                signatures_to_drop.push((signature.signature, signature.tree, signature.leaf_idx));
            }

            if delete_items.len() >= CI_ITEMS_DELETE_BATCH_SIZE {
                self.data_consistency_storage
                    .drop_forked_bubblegum_changes(&changes_to_delete)
                    .await;
                if let Some(changes_tracker) = self.nft_changes_tracker.as_ref() {
                    changes_tracker
                        .watch_remove_forked_bubblegum_changes(&changes_to_delete)
                        .await;
                }
                self.delete_tree_seq_idx(&mut delete_items).await;
            }
        }

        if !delete_items.is_empty() {
            self.data_consistency_storage
                .drop_forked_bubblegum_changes(&changes_to_delete)
                .await;
            if let Some(changes_tracker) = self.nft_changes_tracker.as_ref() {
                changes_tracker
                    .watch_remove_forked_bubblegum_changes(&changes_to_delete)
                    .await;
            }
            self.delete_tree_seq_idx(&mut delete_items).await;
        }

        // once we cleaned tree_seq_idx column family it time to clean cl_items
        // from this column data will be dropped by slot
        // if we have any update from forked slot we have to delete it
        for cl_item in self.cl_items_manager.cl_items_iter() {
            if !rx.is_empty() {
                info!("Stop iteration over cl items iterator...");
                return;
            }

            if !all_non_forked_slots.contains(&cl_item.slot_updated) {
                delete_items.push(ForkedItem {
                    tree: cl_item.cli_tree_key,
                    seq: cl_item.cli_seq,
                    node_idx: cl_item.cli_node_idx,
                });
            }

            if delete_items.len() >= CI_ITEMS_DELETE_BATCH_SIZE {
                self.delete_cl_items(&mut delete_items).await;
            }
        }

        if !delete_items.is_empty() {
            self.delete_cl_items(&mut delete_items).await;
        }

        if !signatures_to_drop.is_empty() {
            self.delete_leaf_signatures(signatures_to_drop).await;
        }

        self.metrics.set_forks_detected(forked_slots as i64);
    }

    async fn delete_tree_seq_idx(&self, delete_items: &mut Vec<ForkedItem>) {
        self.metrics.inc_by_deleted_items(delete_items.len() as u64);
        self.cl_items_manager
            .delete_tree_seq_idx(std::mem::take(delete_items))
            .await;
    }

    async fn delete_cl_items(&self, delete_items: &mut Vec<ForkedItem>) {
        self.metrics.inc_by_deleted_items(delete_items.len() as u64);
        self.cl_items_manager
            .delete_cl_items(std::mem::take(delete_items))
            .await;
    }

    async fn delete_leaf_signatures(&self, keys: Vec<(Signature, Pubkey, u64)>) {
        self.metrics.inc_by_deleted_items(keys.len() as u64);
        self.cl_items_manager.delete_signatures(keys).await;
    }
}
