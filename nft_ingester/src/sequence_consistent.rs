use interface::slots_dumper::SlotsDumper;
use rocks_db::key_encoders::decode_pubkey_u64;
use rocks_db::tree_seq::TreeSeqIdx;
use rocks_db::Storage;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::error;
use usecase::slots_collector::SlotsCollector;

const MAX_KEYS_TO_DELETE_LEN: usize = 5000;

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
        for pair in tree_iterator {
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
                if !keys_to_delete.is_empty() {
                    match self
                        .data_layer
                        .tree_seq_idx
                        .delete_batch(keys_to_delete.clone())
                        .await
                    {
                        Err(e) => error!("delete batch 1: {}", e),
                        Ok(_) => keys_to_delete.clear(),
                    };
                }
                // Collect slots into RocksDB
                // They will be processed together with other slots inside
                // run_perpetual_slot_fetching (we can do such a thing
                // because all txs with any asset tree is also a tx with BBG program)
                self.slots_collector
                    .collect_slots(&format!("{}/", tree), current_slot, slot, rx)
                    .await;
            };

            keys_to_delete.push((current_tree, current_seq));
            if keys_to_delete.len() >= MAX_KEYS_TO_DELETE_LEN {
                match self
                    .data_layer
                    .tree_seq_idx
                    .delete_batch(keys_to_delete.clone())
                    .await
                {
                    Err(e) => error!("delete batch 2: {}", e),
                    Ok(_) => keys_to_delete.clear(),
                };
            }

            tree = current_tree;
            seq = current_seq;
            slot = current_slot;
        }
    }
}
