use std::collections::HashMap;

use entities::models::{AssetSignature, AssetSignatureKey, UpdateVersion, Updated};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, error};

use crate::column::TypedColumn;
use crate::key_encoders::{
    decode_pubkey_u64, decode_u64_pubkey, encode_pubkey_u64, encode_u64_pubkey,
};
use crate::transaction::{CopyableChangeLogEventV1, TreeUpdate};
use crate::tree_seq::TreeSeqIdx;
use crate::{Result, Storage};

/// This column family stores change log items for asset proof construction.
/// Basically, it stores all nodes of the tree.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ClItemDeprecated {
    pub cli_node_idx: u64,
    pub cli_tree_key: Pubkey,
    pub cli_leaf_idx: Option<u64>,
    pub cli_seq: u64,
    pub cli_level: u64,
    pub cli_hash: Vec<u8>,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ClItemV2 {
    pub tree_key: Pubkey,
    pub level: u64,
    pub node_idx: u64,
    pub leaf_idx: Option<u64>,
    pub finalized_hash: Option<Updated<Vec<u8>>>,
    pub pending_hash: Option<Updated<Vec<u8>>>,
}

impl ClItemV2 {
    pub fn get_updated_hash(&self, cutoff_slot: u64) -> Updated<Vec<u8>> {
        match (&self.finalized_hash, &self.pending_hash) {
            (Some(finalized), Some(pending)) => {
                // if the finalized hash is newer than the pending hash, return the finalized hash
                // also return the finalized hash if the pending slot is way behind the last known finalized slot
                if finalized.get_upd_ver_seq() > pending.get_upd_ver_seq()
                    || finalized.slot_updated > pending.slot_updated
                    || pending.slot_updated < cutoff_slot
                {
                    finalized.clone()
                } else {
                    pending.clone()
                }
            }
            (Some(finalized), None) => finalized.clone(),
            (None, Some(pending)) => pending.clone(),
            (None, None) => Updated::new(0, None, vec![]),
        }
    }
}

impl TypedColumn for ClItemV2 {
    type KeyType = ClItemKey;
    // The value type is the change log struct itself
    type ValueType = Self;
    const NAME: &'static str = "CL_ITEMS_V2"; // Name of the column family

    fn encode_key(key: ClItemKey) -> Vec<u8> {
        key.encode_to_bytes_v2()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        ClItemKey::decode_from_bytes_v2(bytes)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SourcedClItemDeprecated {
    pub item: ClItemDeprecated,
    pub is_from_finalized_source: bool,
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

impl TypedColumn for ClItemDeprecated {
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

impl ClItemV2 {
    pub fn merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = existing_val
            .map(bincode::deserialize::<Self>)
            .transpose()
            .map_err(|e| {
                error!("RocksDB: ClItem deserialize existing_val: {}", e);
            })
            .unwrap_or(None);
        for op in operands {
            if let Ok(new_val) = bincode::deserialize::<Self>(op).map_err(|e| {
                error!("RocksDB: ClItem deserialize new_val: {}", e);
            }) {
                if let Some(existing_val) = result.as_mut() {
                    // the finalized value is always merged with the finalized value, same applies to the pending value
                    // if the pending value is the same as the finalized value, the pending is set to None in favor of the finalized value
                    merge_updated(&mut existing_val.finalized_hash, &new_val.finalized_hash);
                    merge_updated(&mut existing_val.pending_hash, &new_val.pending_hash);
                    if existing_val.pending_hash == existing_val.finalized_hash {
                        existing_val.pending_hash = None;
                    }
                } else {
                    result = Some(new_val);
                }
            }
        }
        result
            .map(|r| {
                bincode::serialize(&r).map_err(|e| {
                    error!(
                        "RocksDB: Failed to serialize ClItem from SourcedClItem: {}",
                        e
                    )
                })
            })
            .transpose()
            .unwrap_or(None)
    }
}

fn merge_updated(
    existing_val: &mut Option<Updated<Vec<u8>>>,
    candidate_val: &Option<Updated<Vec<u8>>>,
) {
    if let Some(candidate_val) = candidate_val {
        if let Some(existing_val) = existing_val {
            if existing_val.get_upd_ver_seq() < candidate_val.get_upd_ver_seq() {
                *existing_val = candidate_val.clone();
            }
        } else {
            *existing_val = Some(candidate_val.clone());
        }
    }
}

impl ClItemDeprecated {
    pub fn merge_cl_items(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut cli_seq = -1i64;
        let mut slot = 0u64;

        // Decode existing value as ClItem, since historically only ClItem was stored.
        if let Some(existing_val) = existing_val {
            match bincode::deserialize::<ClItemDeprecated>(existing_val) {
                Ok(value) => {
                    cli_seq = value.cli_seq as i64;
                    slot = value.slot_updated;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: ClItem deserialize existing_val: {}", e);
                }
            }
        }

        let len = operands.len();

        for (i, op) in operands.iter().enumerate() {
            // Try to decode operand as SourcedClItem first
            let new_val = match bincode::deserialize::<SourcedClItemDeprecated>(op) {
                Ok(si) => si,
                Err(_e_sourced) => {
                    // If fails, try decoding as ClItem
                    match bincode::deserialize::<ClItemDeprecated>(op) {
                        Ok(ci) => SourcedClItemDeprecated {
                            item: ci,
                            is_from_finalized_source: false,
                        },
                        Err(e_clitem) => {
                            // If last operand and still no result chosen, store empty if needed
                            if i == len - 1 && result.is_empty() {
                                error!("RocksDB: last operand in ClItem new_val could not be deserialized as SourcedClItem or ClItem. Empty array will be saved: {}", e_clitem);
                                // Return empty array
                                return Some(vec![]);
                            } else {
                                debug!("RocksDB: ClItem deserialize new_val failed: {}", e_clitem);
                            }
                            continue;
                        }
                    }
                }
            };

            let new_slot = new_val.item.slot_updated;
            let new_seq = new_val.item.cli_seq as i64;

            // Compare slots and seq to determine if we should update
            // If the new_val is from a finalized source and outranks by any of slot/seq:
            if new_slot > slot
                || (new_slot == slot && new_seq > cli_seq)
                || (new_val.is_from_finalized_source && new_seq > cli_seq)
            {
                // store only the ClItem portion
                match bincode::serialize(&new_val.item) {
                    Ok(serialized) => {
                        result = serialized;
                        cli_seq = new_seq;
                        slot = new_slot;
                    }
                    Err(e) => {
                        error!(
                            "RocksDB: Failed to serialize ClItem from SourcedClItem: {}",
                            e
                        );
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
        is_from_finalized_source: bool,
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

            let updated_hash = Updated::new(
                slot,
                Some(UpdateVersion::Sequence(change_log_event.seq)),
                p.node.to_vec(),
            );
            let (finalized_hash, pending_hash) = if is_from_finalized_source {
                (Some(updated_hash), None)
            } else {
                // If it’s not from finalized source, we can wrap it directly in pending_hash.
                (None, Some(updated_hash))
            };

            let cl_item = ClItemV2 {
                tree_key: tree,
                level: i,
                node_idx,
                leaf_idx,
                finalized_hash,
                pending_hash,
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

    pub fn encode_to_bytes_v2(&self) -> Vec<u8> {
        encode_pubkey_u64(self.tree_id, self.node_id)
    }

    pub fn decode_from_bytes_v2(bytes: Vec<u8>) -> Result<Self> {
        decode_pubkey_u64(bytes).map(|(tree_id, node_id)| ClItemKey { node_id, tree_id })
    }
}
