use bincode::{deserialize, serialize};
use entities::enums::{OwnerType, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::Updated;
use log::{error, warn};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, pubkey::Pubkey};

use crate::key_encoders::{decode_pubkey, encode_pubkey, encode_u64_pubkey, decode_u64_pubkey};
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
    pub is_compressible: Updated<bool>,
    pub is_compressed: Updated<bool>,
    pub is_frozen: Updated<bool>,
    pub supply: Option<Updated<u64>>,
    pub seq: Option<Updated<u64>>,
    pub is_burnt: Updated<bool>,
    pub was_decompressed: Updated<bool>,
    pub onchain_data: Option<Updated<String>>,
    pub creators: Updated<Vec<entities::models::Creator>>,
    pub royalty_amount: Updated<u16>,
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
    pub owner: Updated<Pubkey>,
    pub delegate: Option<Updated<Pubkey>>,
    pub owner_type: Updated<OwnerType>,
    pub owner_delegate_seq: Option<Updated<u64>>,
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

fn update_field<T: Clone>(current: &mut Updated<T>, new: &Updated<T>) {
    if new.slot_updated > current.slot_updated {
        *current = new.clone();
        return;
    }
    if new.seq.unwrap_or_default() > current.seq.unwrap_or_default() {
        *current = new.clone();
    }
}

fn update_optional_field<T: Clone + Default>(
    current: &mut Option<Updated<T>>,
    new: &Option<Updated<T>>,
) {
    if new.clone().unwrap_or_default().slot_updated
        > current.clone().unwrap_or_default().slot_updated
    {
        *current = new.clone();
        return;
    }
    if new.clone().unwrap_or_default().seq.unwrap_or_default()
        > current.clone().unwrap_or_default().seq.unwrap_or_default()
    {
        *current = new.clone();
    }
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
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
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

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        update_field(&mut current_val.is_compressible, &new_val.is_compressible);
                        update_field(&mut current_val.is_compressed, &new_val.is_compressed);
                        update_field(&mut current_val.is_frozen, &new_val.is_frozen);
                        update_optional_field(&mut current_val.supply, &new_val.supply);
                        update_optional_field(&mut current_val.seq, &new_val.seq);
                        update_field(&mut current_val.is_burnt, &new_val.is_burnt);
                        update_field(&mut current_val.creators, &new_val.creators);
                        update_field(&mut current_val.royalty_amount, &new_val.royalty_amount);
                        update_field(&mut current_val.was_decompressed, &new_val.was_decompressed);
                        update_optional_field(&mut current_val.onchain_data, &new_val.onchain_data);

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

    pub fn get_slot_updated(&self) -> u64 {
        [
            self.is_compressible.slot_updated,
            self.is_compressed.slot_updated,
            self.is_frozen.slot_updated,
            self.supply.clone().map_or(0, |supply| supply.slot_updated),
            self.seq.clone().map_or(0, |seq| seq.slot_updated),
            self.is_burnt.slot_updated,
            self.was_decompressed.slot_updated,
            self.onchain_data
                .clone()
                .map_or(0, |onchain_data| onchain_data.slot_updated),
            self.creators.slot_updated,
            self.royalty_amount.slot_updated,
        ]
        .into_iter()
        .max()
        .unwrap() // unwrap here is safe, because vec is not empty
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
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                }
                Err(e) => {
                    error!("RocksDB: AssetOwner deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        update_field(&mut current_val.owner_type, &new_val.owner_type);
                        update_field(&mut current_val.owner, &new_val.owner);
                        update_optional_field(
                            &mut current_val.owner_delegate_seq,
                            &new_val.owner_delegate_seq,
                        );
                        update_optional_field(&mut current_val.delegate, &new_val.delegate);

                        current_val
                    } else {
                        new_val
                    });
                }
                Err(e) => {
                    error!("RocksDB: AssetOwner deserialize new_val: {}", e)
                }
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }

    pub fn get_slot_updated(&self) -> u64 {
        [
            self.owner.slot_updated,
            self.delegate
                .clone()
                .map_or(0, |delegate| delegate.slot_updated),
            self.owner_type.slot_updated,
            self.owner_delegate_seq
                .clone()
                .map_or(0, |owner_delegate_seq| owner_delegate_seq.slot_updated),
        ]
        .into_iter()
        .max()
        .unwrap() // unwrap here is safe, because vec is not empty
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

#[derive(Serialize, Deserialize, Debug)]
pub struct SlotAssetIdx{}

impl TypedColumn for SlotAssetIdx {
    type KeyType = (u64, Pubkey);
    type ValueType = Self;
    const NAME: &'static str = "SLOT_ASSET_IDX";

    fn encode_key(key: (u64, Pubkey)) -> Vec<u8> {
        encode_u64_pubkey(key.0, key.1)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_u64_pubkey(bytes)
    }
}
    