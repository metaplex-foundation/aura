use interface::slots_dumper::SlotsDumper;
use rocks_db::key_encoders::decode_pubkey_u64;
use rocks_db::tree_seq::TreeSeqIdx;
use rocks_db::Storage;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};
use usecase::slots_collector::SlotsCollector;

pub struct SequenceConsistentGapfiller<T>
where
    T: SlotsDumper,
{
    data_layer: Arc<Storage>,
    slots_collector: SlotsCollector<T>,
}

impl<T> SequenceConsistentGapfiller<T>
where
    T: SlotsDumper,
{
    pub fn new(data_layer: Arc<Storage>, slots_collector: SlotsCollector<T>) -> Self {
        Self {
            data_layer,
            slots_collector,
        }
    }

    pub async fn start_sequence_gapfill(&self, rx: &mut Receiver<()>) {
        let tree_iterator = self.data_layer.tree_seq_idx.iter_start();
        let mut keys_to_delete = Vec::new();
        let mut tree = solana_program::pubkey::Pubkey::default();
        let mut seq = 0;
        let mut slot = 0;
        let mut keys_already_deleted_for_tree = false;
        for pair in tree_iterator {
            if !rx.is_empty() {
                info!("Stop iteration over tree iterator...");
                return;
            }
            let (key, value) = match pair {
                Ok(pair) => pair,
                Err(e) => {
                    error!("Get tree with seq pair: {}", e);
                    continue;
                }
            };
            let (current_tree, current_seq) = match decode_pubkey_u64(key.to_vec()) {
                Ok((current_tree, current_seq)) => (current_tree, current_seq),
                Err(e) => {
                    error!("decode_pubkey_u64: {}", e);
                    continue;
                }
            };
            let current_slot = match bincode::deserialize::<TreeSeqIdx>(value.as_ref()) {
                Ok(slot) => slot.slot,
                Err(e) => {
                    error!("deserialize slot: {}", e);
                    continue;
                }
            };
            if tree == current_tree && current_seq != seq + 1 {
                // Delete all previous slots
                if keys_to_delete.len() > 1 {
                    match self
                        .data_layer
                        .tree_seq_idx
                        // do not delete last element, otherwise we cannot indicate gap in future
                        .delete_range(keys_to_delete[0], keys_to_delete[keys_to_delete.len() - 2])
                        .await
                    {
                        Err(e) => error!("delete range: {}", e),
                        Ok(_) => {
                            keys_already_deleted_for_tree = true;
                            keys_to_delete.clear()
                        }
                    }
                }
                self.slots_collector
                    .collect_slots(
                        &format!("{}/", tree),
                        current_slot,
                        slot,
                        &mut rx.resubscribe(),
                    )
                    .await;
            };
            if tree != current_tree {
                keys_already_deleted_for_tree = false
            }
            // If keys already deleted for some tree, we must not to delete other keys in this tree
            // in order to save gap and in future check, if we fix it
            if !keys_already_deleted_for_tree {
                keys_to_delete.push((current_tree, current_seq));
            }
            tree = current_tree;
            seq = current_seq;
            slot = current_slot;
        }
    }
}
