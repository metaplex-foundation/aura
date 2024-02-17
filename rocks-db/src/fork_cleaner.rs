use crate::Storage;
use async_trait::async_trait;
use entities::models::{ClItem, ForkedItem};
use interface::fork_cleaner::{ClItemsManager, ForkChecker};
use tracing::error;

const SLOT_CHECK_OFFSET: u64 = 1000;

#[async_trait]
impl ClItemsManager for Storage {
    fn items_iter(&self) -> impl Iterator<Item = ClItem> {
        self.cl_items
            .iter_start()
            .filter_map(Result::ok)
            .flat_map(|(_, value)| bincode::deserialize::<ClItem>(value.as_ref()))
    }

    async fn delete_items(&self, keys: Vec<ForkedItem>) {
        let (cl_items_res, tree_seq_idx_res) = tokio::join!(
            self.cl_items
                .delete_batch(keys.iter().map(|key| (key.node_idx, key.tree)).collect()),
            // Indicate gap in sequences, so SequenceConsistentChecker will fill it in future
            self.tree_seq_idx
                .delete_batch(keys.iter().map(|key| (key.tree, key.seq)).collect())
        );
        for res in [
            (cl_items_res, "Cl items delete"),
            (tree_seq_idx_res, "Tree sequence delete"),
        ] {
            if let Err(e) = res.0 {
                error!("{}: {}", res.1, e);
            }
        }
    }
}

#[async_trait]
impl ForkChecker for Storage {
    async fn is_forked_slot(&self, slot: u64) -> bool {
        match self.raw_blocks_cbor.has_key(slot).await {
            Ok(has_key) => !has_key,
            Err(e) => {
                error!("Check raw blocks has key {}", e);
                true
            }
        }
    }

    fn last_slot_for_check(&self) -> u64 {
        for (key, _) in self.raw_blocks_cbor.iter_end().filter_map(Result::ok) {
            match crate::key_encoders::decode_u64(key.to_vec()) {
                Ok(key) => return key.saturating_sub(SLOT_CHECK_OFFSET),
                Err(e) => {
                    error!("Decode raw block key: {}", e);
                }
            };
        }
        // if there no saved block - we cannot do any check
        0
    }
}
