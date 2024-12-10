use std::collections::HashMap;

use bincode::deserialize;
use entities::models::{AssetSignature, AssetSignatureKey};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::events::ChangeLogEventV1;
use tracing::{debug, error, warn};

use crate::column::TypedColumn;
use crate::key_encoders::{decode_u64_pubkey, encode_u64_pubkey};
use crate::transaction::{CopyableChangeLogEventV1, TreeUpdate};
use crate::tree_seq::TreeSeqIdx;
use crate::{Result, Storage};

/// This column family stores change log items for asset proof construction.
/// Basically, it stores all nodes of the tree.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ClItem {
    pub cli_node_idx: u64,
    pub cli_tree_key: Pubkey,
    pub cli_leaf_idx: Option<u64>,
    pub cli_seq: u64,
    pub cli_level: u64,
    pub cli_hash: Vec<u8>,
    pub slot_updated: u64,
}

/// This column family stores node ids of the leaf nodes.
/// The key is the leaf index(also known as nonce) and tree id.
/// NOTE: it stores only nodes with level 0 in tree.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClLeaf {
    pub cli_leaf_idx: u64,
    pub cli_tree_key: Pubkey,
    pub cli_node_idx: u64,
}

impl TypedColumn for ClItem {
    type KeyType = ClItemKey;
    // The value type is the change log struct itself
    type ValueType = Self;
    const NAME: &'static str = "CL_ITEMS"; // Name of the column family

    fn encode_key(key: ClItemKey) -> Vec<u8> {
        key.encode_to_bytes()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        ClItemKey::decode_from_bytes(bytes)
    }
}

impl ClItem {
    pub fn merge_cl_items(
        new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut cli_seq = -1;
        let mut slot = 0;
        if let Some(existing_val) = existing_val {
            match deserialize::<ClItem>(existing_val) {
                Ok(value) => {
                    cli_seq = value.cli_seq as i64;
                    slot = value.slot_updated;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: ClItem deserialize existing_val: {}", e)
                }
            }
        }

        let len = operands.len();

        for (i, op) in operands.iter().enumerate() {
            match deserialize::<ClItem>(op) {
                Ok(new_val) => {
                    if new_val.slot_updated > slot
                        || (new_val.slot_updated == slot && new_val.cli_seq as i64 > cli_seq)
                    {
                        if new_val.slot_updated > slot && (new_val.cli_seq as i64) < cli_seq {
                            warn!("RocksDB: ClItem new_val slot {} is greater than existing slot {}, but seq {} is less than the exising {} for {}.", new_val.slot_updated, slot, new_val.cli_seq, cli_seq, bs58::encode(new_key));
                        }
                        cli_seq = new_val.cli_seq as i64;
                        slot = new_val.slot_updated;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    if i == len - 1 && result.is_empty() {
                        error!("RocksDB: last operand in ClItem new_val could not be deserialized. Empty array will be saved: {}", e)
                    } else {
                        debug!("RocksDB: ClItem deserialize new_val: {}", e);
                    }
                }
            }
        }

        Some(result)
    }
}

impl TypedColumn for ClLeaf {
    type KeyType = ClLeafKey;
    // The value type is the leaf node id
    type ValueType = Self;
    const NAME: &'static str = "CL_LEAF"; // Name of the column family

    fn encode_key(key: ClLeafKey) -> Vec<u8> {
        encode_u64_pubkey(key.node_id, key.tree_id)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        ClLeafKey::decode_from_bytes(bytes)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct ClLeafKey {
    pub node_id: u64,
    pub tree_id: Pubkey,
}

impl ClLeafKey {
    pub fn new(node_id: u64, tree_id: Pubkey) -> ClLeafKey {
        ClLeafKey { node_id, tree_id }
    }
    pub fn encode_ty_bytes(&self) -> Vec<u8> {
        encode_u64_pubkey(self.node_id, self.tree_id)
    }
    pub fn decode_from_bytes(bytes: Vec<u8>) -> Result<ClLeafKey> {
        decode_u64_pubkey(bytes).map(|(node_id, tree_id)| ClLeafKey { node_id, tree_id })
    }
}

impl Storage {
    pub async fn save_changelog(&self, change_log_event: &ChangeLogEventV1, slot: u64) {
        let tree = change_log_event.id;

        let mut i: u64 = 0;
        let depth = change_log_event.path.len() - 1;
        let mut items_map = HashMap::new();
        let mut leaf_map = HashMap::new();
        for p in change_log_event.path.iter() {
            let node_idx = p.index as u64;

            let leaf_idx = if i == 0 {
                Some(node_idx_to_leaf_idx(node_idx, depth as u32))
            } else {
                None
            };

            i += 1;

            let cl_item = ClItem {
                cli_node_idx: node_idx,
                cli_tree_key: tree,
                cli_leaf_idx: leaf_idx,
                cli_seq: change_log_event.seq,
                cli_level: i,
                cli_hash: p.node.to_vec(),
                slot_updated: slot,
            };

            items_map.insert(ClItemKey::new(node_idx, tree), cl_item);
            // save leaf's node id
            if i == 1 {
                if let Some(leaf_idx) = leaf_idx {
                    let cl_leaf = ClLeaf {
                        cli_leaf_idx: leaf_idx,
                        cli_tree_key: tree,
                        cli_node_idx: node_idx,
                    };
                    leaf_map.insert(ClLeafKey::new(leaf_idx, tree), cl_leaf);
                }
            }
        }
        let merge_res = self.cl_items.merge_batch(items_map);
        let put_res = self.cl_leafs.put_batch(leaf_map);
        if let Err(e) = merge_res.await {
            error!("Error while saving change log for cNFT: {}", e);
        };
        if let Err(e) = put_res.await {
            error!("Error while saving change log for cNFT: {}", e);
        }
    }

    pub(crate) fn save_tree_with_batch(&self, batch: &mut rocksdb::WriteBatch, tree: &TreeUpdate) {
        if let Err(e) = self.tree_seq_idx.put_with_batch(
            batch,
            (tree.tree, tree.seq),
            &TreeSeqIdx { slot: tree.slot },
        ) {
            error!("Error while saving tree update: {}", e);
        };
    }

    pub(crate) fn save_changelog_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        change_log_event: &CopyableChangeLogEventV1,
        slot: u64,
    ) {
        let tree = change_log_event.id;

        let mut i: u64 = 0;
        let depth = change_log_event.path.len() - 1;
        let mut leaf_map = HashMap::new();
        for p in change_log_event.path.iter() {
            let node_idx = p.index as u64;

            let leaf_idx = if i == 0 {
                Some(node_idx_to_leaf_idx(node_idx, depth as u32))
            } else {
                None
            };

            i += 1;

            let cl_item = ClItem {
                cli_node_idx: node_idx,
                cli_tree_key: tree,
                cli_leaf_idx: leaf_idx,
                cli_seq: change_log_event.seq,
                cli_level: i,
                cli_hash: p.node.to_vec(),
                slot_updated: slot,
            };

            if let Err(e) =
                self.cl_items
                    .merge_with_batch(batch, ClItemKey::new(node_idx, tree), &cl_item)
            {
                error!("Error while saving change log for cNFT: {}", e);
            };

            // save leaf's node id
            if i == 1 {
                if let Some(leaf_idx) = leaf_idx {
                    let cl_leaf = ClLeaf {
                        cli_leaf_idx: leaf_idx,
                        cli_tree_key: tree,
                        cli_node_idx: node_idx,
                    };
                    leaf_map.insert((leaf_idx, tree), cl_leaf.clone());
                    if let Err(e) = self.cl_leafs.put_with_batch(
                        batch,
                        ClLeafKey::new(leaf_idx, tree),
                        &cl_leaf,
                    ) {
                        error!("Error while saving change log for cNFT: {}", e);
                    };
                }
            }
        }
    }

    pub(crate) fn save_asset_signature_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        tree: &TreeUpdate,
    ) {
        if let Err(e) = self.asset_signature.put_with_batch(
            batch,
            AssetSignatureKey {
                tree: tree.tree,
                leaf_idx: tree.event.leaf_id as u64,
                seq: tree.seq,
            },
            &AssetSignature {
                tx: tree.tx.clone(),
                instruction: tree.instruction.clone(),
                slot: tree.slot,
            },
        ) {
            error!("Error while saving tree update: {}", e);
        };
    }
}

fn node_idx_to_leaf_idx(index: u64, tree_height: u32) -> u64 {
    index - 2u64.pow(tree_height)
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct ClItemKey {
    pub node_id: u64,
    pub tree_id: Pubkey,
}

impl ClItemKey {
    pub fn new(node_id: u64, tree_id: Pubkey) -> ClItemKey {
        ClItemKey { node_id, tree_id }
    }

    fn encode_to_bytes(&self) -> Vec<u8> {
        encode_u64_pubkey(self.node_id, self.tree_id)
    }

    fn decode_from_bytes(bytes: Vec<u8>) -> Result<Self> {
        decode_u64_pubkey(bytes).map(|(node_id, tree_id)| ClItemKey { node_id, tree_id })
    }
}
