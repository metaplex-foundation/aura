use std::collections::HashSet;

use async_trait::async_trait;
use entities::models::{ClItem, ForkedItem, LeafSignatureAllData, RawBlock};
use interface::fork_cleaner::{CompressedTreeChangesManager, ForkChecker};
use rocksdb::IteratorMode;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};

use crate::{
    cl_items::ClItemKey,
    column::TypedColumn,
    columns::{cl_items::ClItemV2, leaf_signatures::LeafSignature},
    SlotStorage, Storage, DROP_ACTION, FULL_ITERATION_ACTION, ITERATOR_TOP_ACTION,
    RAW_BLOCKS_CBOR_ENDPOINT, ROCKS_COMPONENT,
};

#[async_trait]
impl CompressedTreeChangesManager for Storage {
    fn tree_seq_idx_iter(&self) -> impl Iterator<Item = LeafSignatureAllData> {
        self.leaf_signature.iter_start().filter_map(Result::ok).flat_map(|(key, value)| {
            match LeafSignature::decode_key(key.to_vec()) {
                Ok((signature, tree, leaf_idx)) => {
                    match bincode::deserialize::<LeafSignature>(value.as_ref()) {
                        Ok(slot_sequences) => Ok(LeafSignatureAllData {
                            tree,
                            signature,
                            leaf_idx,
                            slot_sequences: slot_sequences.data,
                        }),
                        Err(e) => Err(format!("Value deserialization error: {:?}", e.to_string())),
                    }
                },
                Err(e) => Err(format!("Key deserialization error: {:?}", e.to_string())),
            }
        })
    }

    fn cl_items_iter(&self) -> impl Iterator<Item = ClItem> {
        self.cl_items
            .iter_start()
            .filter_map(Result::ok)
            .flat_map(|(_, value)| bincode::deserialize::<ClItemV2>(value.as_ref()))
            .map(|cl_item_v2| {
                // TODO: using pending always if the fork cleaner is ever used, this should be revised
                let hash = cl_item_v2.get_updated_hash(0);
                ClItem {
                    cli_node_idx: cl_item_v2.node_idx,
                    cli_tree_key: cl_item_v2.tree_key,
                    cli_leaf_idx: cl_item_v2.leaf_idx,
                    cli_seq: hash.get_upd_ver_seq().unwrap_or_default(),
                    cli_level: cl_item_v2.level,
                    cli_hash: hash.value,
                    slot_updated: hash.slot_updated,
                }
            })
    }

    async fn delete_tree_seq_idx(&self, keys: Vec<ForkedItem>) {
        let start_time = chrono::Utc::now();

        if let Err(e) = self
            .tree_seq_idx
            .delete_batch(keys.iter().map(|key| (key.tree, key.seq)).collect())
            .await
        {
            error!("Tree sequence delete: {}", e.to_string());
        }

        self.red_metrics.observe_request(ROCKS_COMPONENT, DROP_ACTION, "tree_seq_idx", start_time);
    }

    async fn delete_cl_items(&self, keys: Vec<ForkedItem>) {
        let start_time = chrono::Utc::now();

        if let Err(e) = self
            .cl_items
            .delete_batch(keys.iter().map(|key| ClItemKey::new(key.node_idx, key.tree)).collect())
            .await
        {
            error!("Cl items delete: {}", e.to_string());
        }

        self.red_metrics.observe_request(ROCKS_COMPONENT, DROP_ACTION, "cl_items", start_time);
    }

    async fn delete_signatures(&self, keys: Vec<(Signature, Pubkey, u64)>) {
        let start_time = chrono::Utc::now();

        if let Err(e) = self.leaf_signature.delete_batch(keys).await {
            error!("Leaf signatures delete: {}", e.to_string());
        }

        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            DROP_ACTION,
            "leaf_signature",
            start_time,
        );
    }
}

#[async_trait]
impl ForkChecker for SlotStorage {
    fn get_all_non_forked_slots(&self, rx: Receiver<()>) -> HashSet<u64> {
        let start_time = chrono::Utc::now();
        let mut all_keys = HashSet::new();
        for (key, _) in self
            .db
            .full_iterator_cf(&self.db.cf_handle(RawBlock::NAME).unwrap(), IteratorMode::Start)
            .filter_map(Result::ok)
        {
            if !rx.is_empty() {
                info!("Stop iteration over raw_blocks_cbor iterator...");
                return all_keys;
            }
            match crate::key_encoders::decode_u64(key.to_vec()) {
                Ok(key) => all_keys.insert(key),
                Err(e) => {
                    error!("Decode raw block key: {}", e);
                    continue;
                },
            };
        }
        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            FULL_ITERATION_ACTION,
            RAW_BLOCKS_CBOR_ENDPOINT,
            start_time,
        );

        all_keys
    }

    fn last_slot_for_check(&self) -> u64 {
        let start_time = chrono::Utc::now();
        let mut it = self.db.raw_iterator_cf(&self.db.cf_handle(RawBlock::NAME).unwrap());
        it.seek_to_last();
        if !it.valid() {
            self.red_metrics.observe_request(
                ROCKS_COMPONENT,
                ITERATOR_TOP_ACTION,
                RAW_BLOCKS_CBOR_ENDPOINT,
                start_time,
            );
            // if there are no saved blocks - we can not do any checks
            return 0;
        }
        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            ITERATOR_TOP_ACTION,
            RAW_BLOCKS_CBOR_ENDPOINT,
            start_time,
        );
        it.key().and_then(|b| RawBlock::decode_key(b.to_vec()).ok()).unwrap_or_default()
    }
}
