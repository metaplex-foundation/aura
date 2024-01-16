use bincode::deserialize;
use log::error;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::events::ChangeLogEventV1;

use crate::asset::AssetLeaf;
use crate::column::TypedColumn;
use crate::key_encoders::{decode_u64_pubkey, encode_u64_pubkey};
use crate::{AssetDynamicDetails, Result, Storage};

/// This column family stores change log items for asset proof construction.
/// Basically, it stores all nodes of the tree.
#[derive(Serialize, Deserialize, Debug, Clone)]
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
    type KeyType = (u64, Pubkey);
    // The value type is the change log struct itself
    type ValueType = Self;
    const NAME: &'static str = "CL_ITEMS"; // Name of the column family

    fn encode_key((node_id, tree_id): (u64, Pubkey)) -> Vec<u8> {
        encode_u64_pubkey(node_id, tree_id)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_u64_pubkey(bytes)
    }
}

impl ClItem {
    pub fn merge_cl_items(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut cli_seq = 0;
        if let Some(existing_val) = existing_val {
            match deserialize::<ClItem>(existing_val) {
                Ok(value) => {
                    cli_seq = value.cli_seq;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: ClItem deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<ClItem>(op) {
                Ok(new_val) => {
                    if new_val.cli_seq > cli_seq {
                        cli_seq = new_val.cli_seq;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: ClItem deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}

impl TypedColumn for ClLeaf {
    type KeyType = (u64, Pubkey);
    // The value type is the leaf node id
    type ValueType = Self;
    const NAME: &'static str = "CL_LEAF"; // Name of the column family

    fn encode_key((node_id, tree_id): (u64, Pubkey)) -> Vec<u8> {
        encode_u64_pubkey(node_id, tree_id)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_u64_pubkey(bytes)
    }
}

impl Storage {
    pub async fn save_changelog(&self, change_log_event: &ChangeLogEventV1, slot: u64) {
        let tree = change_log_event.id;

        let mut i: u64 = 0;
        let depth = change_log_event.path.len() - 1;
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

            if let Err(e) = self.cl_items.merge((node_idx, tree), &cl_item) {
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

                    if let Err(e) = self.cl_leafs.put((leaf_idx, tree), &cl_leaf) {
                        error!("Error while saving change log for cNFT: {}", e);
                    };
                }
            }
        }
    }

    pub fn save_tx_data_and_asset_updated(
        &self,
        pk: Pubkey,
        slot: u64,
        leaf: Option<AssetLeaf>,
        dynamic_data: Option<AssetDynamicDetails>,
    ) -> Result<()> {
        if let Some(leaf) = leaf {
            self.asset_leaf_data.merge(pk, &leaf)?
        };
        if let Some(dynamic_data) = dynamic_data {
            self.asset_dynamic_data.merge(pk, &dynamic_data)?;
        }
        self.asset_updated(slot, pk)?;

        Ok(())
    }
}

fn node_idx_to_leaf_idx(index: u64, tree_height: u32) -> u64 {
    index - 2u64.pow(tree_height)
}
