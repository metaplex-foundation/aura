use bincode::{deserialize, serialize};
use entities::enums::{SpecificationAssetClass, RoyaltyTargetType, OwnerType};
use log::{error, warn};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, pubkey::Pubkey};

use crate::key_encoders::{decode_pubkey, encode_pubkey};
use crate::Result;
use crate::TypedColumn;

// The following structures are used to store the asset data in the rocksdb database. The data is spread across multiple columns based on the update pattern.
// The final representation of the asset should be reconstructed by querying the database for the asset and its associated columns.
// Some values, like slot_updated should be calculated based on the latest update to the asset.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetStaticDetails {
    pub pubkey: Pubkey,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetDynamicDetails {
    pub pubkey: Pubkey,
    pub is_compressible: (u64, bool),
    pub is_compressed: (u64, bool),
    pub is_frozen: (u64, bool),
    pub supply: (u64, Option<u64>),
    pub seq: (u64, Option<u64>),
    pub is_burnt: (u64, bool),
    pub was_decompressed: (u64, bool),
    pub onchain_data: (u64, Option<String>),
    pub creators: (u64, Vec<entities::models::Creator>),
    pub royalty_amount: (u64, u16),
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetAuthority {
    pub pubkey: Pubkey,
    pub authority: Pubkey,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetOwner {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub delegate: Option<Pubkey>,
    pub owner_type: OwnerType,
    pub owner_delegate_seq: Option<u64>,
    pub slot_updated: u64,
}

/// Leaf information about compressed asset
/// Nonce - is basically the leaf index. It takes from tree supply.
/// NOTE: leaf index is not the same as node index. Leaf index is specifically the index of the leaf in the tree.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetLeaf {
    pub pubkey: Pubkey,
    pub tree_id: Pubkey,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<u64>,
    pub data_hash: Option<Hash>,
    pub creator_hash: Option<Hash>,
    pub leaf_seq: Option<u64>,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetCollection {
    pub pubkey: Pubkey,
    pub collection: Pubkey,
    pub is_collection_verified: bool,
    pub collection_seq: Option<u64>,
    pub slot_updated: u64,
}

impl TypedColumn for AssetStaticDetails {
    type KeyType = Pubkey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "ASSET_STATIC"; // Name of the column family

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetDynamicDetails {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_DYNAMIC";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetAuthority {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_AUTHORITY";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

/// Not a real merge operation. We just check if static info
/// already exist and if so we don't overwrite it.
impl AssetStaticDetails {
    pub fn merge_static_details(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        if let Some(existing_val) = existing_val {
            result = existing_val.to_vec();

            return Some(result);
        }

        if let Some(op) = operands.into_iter().next() {
            result = op.to_vec();

            return Some(result);
        }

        Some(result)
    }
}

impl AssetDynamicDetails {
    pub fn merge_dynamic_details(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Option<AssetDynamicDetails> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetDynamicDetails>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                }
                Err(e) => {
                    error!(
                        "RocksDB: AssetDynamicDetails deserialize existing_val: {}",
                        e
                    )
                }
            }
        }

        fn update_field<T: Clone + PartialEq>(current: &mut (u64, T), new: &(u64, T)) {
            if new.0 > current.0 {
                *current = new.clone();
            }
        }

        for op in operands {
            match deserialize::<AssetDynamicDetails>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        update_field(&mut current_val.is_compressible, &new_val.is_compressible);
                        update_field(&mut current_val.is_compressed, &new_val.is_compressed);
                        update_field(&mut current_val.is_frozen, &new_val.is_frozen);
                        update_field(&mut current_val.supply, &new_val.supply);
                        update_field(&mut current_val.seq, &new_val.seq);
                        update_field(&mut current_val.is_burnt, &new_val.is_burnt);
                        update_field(&mut current_val.creators, &new_val.creators);
                        update_field(&mut current_val.royalty_amount, &new_val.royalty_amount);
                        update_field(&mut current_val.was_decompressed, &new_val.was_decompressed);
                        update_field(&mut current_val.onchain_data, &new_val.onchain_data);

                        current_val
                    } else {
                        new_val
                    });
                }
                Err(e) => {
                    error!("RocksDB: AssetDynamicDetails deserialize new_val: {}", e)
                }
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }
}

impl AssetAuthority {
    pub fn merge_asset_authorities(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetAuthority>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: AssetAuthority deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<AssetAuthority>(op) {
                Ok(new_val) => {
                    if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: AssetAuthority deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}

impl TypedColumn for AssetOwner {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_OWNER";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl AssetOwner {
    pub fn merge_asset_owner(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        let mut owner_delegate_seq = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetOwner>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    owner_delegate_seq = value.owner_delegate_seq;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: AssetOwner deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<AssetOwner>(op) {
                Ok(new_val) => {
                    if let Some(current_seq) = owner_delegate_seq {
                        if let Some(new_seq) = new_val.owner_delegate_seq {
                            if new_seq > current_seq {
                                owner_delegate_seq = new_val.owner_delegate_seq;
                                result = op.to_vec();
                            }
                        } else {
                            warn!("RocksDB: AssetOwner deserialize new_val: new owner_delegate_seq is None");
                        }
                    } else if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: AssetOwner deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}

impl TypedColumn for AssetLeaf {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_LEAF";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl AssetLeaf {
    pub fn merge_asset_leaf(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        let mut leaf_seq = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetLeaf>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    leaf_seq = value.leaf_seq;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: AssetLeaf deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<AssetLeaf>(op) {
                Ok(new_val) => {
                    if let Some(current_seq) = leaf_seq {
                        if let Some(new_seq) = new_val.leaf_seq {
                            if new_seq > current_seq {
                                leaf_seq = new_val.leaf_seq;
                                result = op.to_vec();
                            }
                        } else {
                            warn!("RocksDB: AssetLeaf deserialize new_val: new leaf_seq is None");
                        }
                    } else if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: AssetLeaf deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}

impl TypedColumn for AssetCollection {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_COLLECTION";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl AssetCollection {
    pub fn merge_asset_collection(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        let mut collection_seq = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetCollection>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    collection_seq = value.collection_seq;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<AssetCollection>(op) {
                Ok(new_val) => {
                    if let Some(current_seq) = collection_seq {
                        if let Some(new_seq) = new_val.collection_seq {
                            if new_seq > current_seq {
                                collection_seq = new_val.collection_seq;
                                result = op.to_vec();
                            }
                        } else {
                            warn!("RocksDB: AssetCollection deserialize new_val: new collection_seq is None");
                        }
                    } else if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}

// AssetsUpdateIdx is a column family that is used to query the assets that were updated in (or after) a particular slot.
// The key is a concatenation of the slot and the asset pubkey.
// This will be used in the batch updater to the secondary index database. The batches should be constructed based on the slot, with an overlap of 1 slot including the last processed slot.
#[derive(Serialize, Deserialize, Debug)]
pub struct AssetsUpdateIdx {}

impl TypedColumn for AssetsUpdateIdx {
    type KeyType = Vec<u8>;
    type ValueType = Self;
    const NAME: &'static str = "ASSETS_UPDATED_IN_SLOT_IDX";

    fn encode_key(key: Vec<u8>) -> Vec<u8> {
        key
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        Ok(bytes)
    }
}

// Asset modifications as of now:

// upsert_assets - all of the fields
// asset_decompress - ast_tree_id, ast_leaf, ast_nonce,
//                                         ast_data_hash, ast_creator_hash, ast_is_compressed,
//                                         ast_supply, ast_was_decompressed, ast_seq
// asset_mint - ast_is_compressed, ast_is_frozen, ast_supply, ast_tree_id,
//                                         ast_nonce, ast_royalty_target_type, ast_royalty_target, ast_royalty_amount, ast_is_burnt,
//                                         ast_slot_updated, ast_was_decompressed, ast_is_collection_verified, ast_specification_asset_class, ast_onchain_data

// upsert_asset_seq - ast_seq
// mark_asset_as_burned - ast_is_burnt, ast_seq, ast_is_compressed;          ast_is_collection_verified

// update_asset_collection_info - ast_collection, ast_is_collection_verified, ast_collection_seq
// update_asset_leaf_info - ast_nonce, ast_tree_id, ast_leaf, ast_leaf_seq, ast_data_hash, ast_creator_hash;        ast_is_collection_verified, ast_is_compressed, ast_is_frozen
// update_asset_owner_and_delegate_info - ast_owner, ast_delegate, ast_owner_delegate_seq;           ast_is_compressed
// update_authority - ast_authority
