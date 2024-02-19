use crate::Storage;
use async_trait::async_trait;
use entities::models::{ClItem, ForkedItem};
use interface::fork_cleaner::{ClItemsManager, ForkChecker};
use std::collections::HashSet;
use tracing::error;

const ROCKS_COMPONENT: &str = "rocks_db";
const DROP_ACTION: &str = "drop";
const RAW_BLOCKS_CBOR_ENDPOINT: &str = "raw_blocks_cbor";

#[async_trait]
impl ClItemsManager for Storage {
    fn items_iter(&self) -> impl Iterator<Item = ClItem> {
        self.cl_items
            .iter_start()
            .filter_map(Result::ok)
            .flat_map(|(_, value)| bincode::deserialize::<ClItem>(value.as_ref()))
    }

    async fn delete_items(&self, keys: Vec<ForkedItem>) {
        let start_time = chrono::Utc::now();
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
        self.red_metrics
            .observe_request(ROCKS_COMPONENT, DROP_ACTION, "cl_items", start_time);
    }
}

#[async_trait]
impl ForkChecker for Storage {
    fn get_all_non_forked_slots(&self) -> HashSet<u64> {
        let start_time = chrono::Utc::now();
        let mut all_keys = HashSet::new();
        for (key, _) in self.raw_blocks_cbor.iter_start().filter_map(Result::ok) {
            match crate::key_encoders::decode_u64(key.to_vec()) {
                Ok(key) => all_keys.insert(key),
                Err(e) => {
                    error!("Decode raw block key: {}", e);
                    continue;
                }
            };
        }
        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            "iterator_top",
            RAW_BLOCKS_CBOR_ENDPOINT,
            start_time,
        );

        all_keys
    }

    fn last_slot_for_check(&self) -> u64 {
        let start_time = chrono::Utc::now();
        for (key, _) in self.raw_blocks_cbor.iter_end().filter_map(Result::ok) {
            match crate::key_encoders::decode_u64(key.to_vec()) {
                Ok(key) => return key,
                Err(e) => {
                    error!("Decode raw block key: {}", e);
                }
            };
        }
        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            "full_iteration",
            RAW_BLOCKS_CBOR_ENDPOINT,
            start_time,
        );
        // if there no saved block - we cannot do any check
        0
    }
}
