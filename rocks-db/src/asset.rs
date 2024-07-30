use std::collections::HashMap;

use bincode::{deserialize, serialize};
use entities::enums::{ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::{EditionData, OffChainData, UpdateVersion, Updated};
use log::{error, warn};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::cmp::Ordering;

use crate::key_encoders::{decode_pubkey, decode_u64_pubkey, encode_pubkey, encode_u64_pubkey};
use crate::Result;
use crate::TypedColumn;

#[derive(Debug)]
pub struct AssetSelectedMaps {
    pub assets_static: HashMap<Pubkey, AssetStaticDetails>,
    pub assets_dynamic: HashMap<Pubkey, AssetDynamicDetails>,
    pub assets_authority: HashMap<Pubkey, AssetAuthority>,
    pub assets_collection: HashMap<Pubkey, AssetCollection>,
    pub assets_owner: HashMap<Pubkey, AssetOwner>,
    pub assets_leaf: HashMap<Pubkey, AssetLeaf>,
    pub offchain_data: HashMap<String, OffChainData>,
    pub urls: HashMap<String, String>,
    pub editions: HashMap<Pubkey, EditionData>,
    pub mpl_core_collections: HashMap<Pubkey, AssetCollection>,
}

// The following structures are used to store the asset data in the rocksdb database. The data is spread across multiple columns based on the update pattern.
// The final representation of the asset should be reconstructed by querying the database for the asset and its associated columns.
// Some values, like slot_updated should be calculated based on the latest update to the asset.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetStaticDetails {
    pub pubkey: Pubkey,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub created_at: i64,
    pub edition_address: Option<Pubkey>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetStaticDetailsDeprecated {
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
    pub url: Updated<String>,
    pub chain_mutability: Option<Updated<ChainMutability>>,
    pub lamports: Option<Updated<u64>>,
    pub executable: Option<Updated<bool>>,
    pub metadata_owner: Option<Updated<String>>,
    pub raw_name: Option<Updated<String>>,
    pub mpl_core_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_plugins: Option<Updated<String>>,
    pub rent_epoch: Option<Updated<u64>>,
    pub num_minted: Option<Updated<u32>>,
    pub current_size: Option<Updated<u32>>,
    pub plugins_json_version: Option<Updated<u32>>,
    pub mpl_core_external_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_external_plugins: Option<Updated<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetDynamicDetailsDeprecated {
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
    pub url: Updated<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetadataMintMap {
    // this is Metadata acc pubkey
    // it's PDA with next seeds ["metadata", metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s, mint_key]
    pub pubkey: Pubkey,
    pub mint_key: Pubkey,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetAuthority {
    pub pubkey: Pubkey,
    pub authority: Pubkey,
    pub slot_updated: u64,
    pub write_version: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetAuthorityDeprecated {
    pub pubkey: Pubkey,
    pub authority: Pubkey,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetOwner {
    pub pubkey: Pubkey,
    pub owner: Updated<Option<Pubkey>>,
    pub delegate: Updated<Option<Pubkey>>,
    pub owner_type: Updated<OwnerType>,
    pub owner_delegate_seq: Updated<Option<u64>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetOwnerDeprecated {
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
    pub collection: Updated<Pubkey>,
    pub is_collection_verified: Updated<bool>,
    pub authority: Updated<Option<Pubkey>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetCollectionDeprecated {
    pub pubkey: Pubkey,
    pub collection: Pubkey,
    pub is_collection_verified: bool,
    pub collection_seq: Option<u64>,
    pub slot_updated: u64,
}

pub(crate) fn update_field<T: Clone>(current: &mut Updated<T>, new: &Updated<T>) {
    if current.update_version.is_some() && new.update_version.is_some() {
        match current
            .update_version
            .clone()
            .unwrap()
            .partial_cmp(&new.update_version.clone().unwrap())
        {
            Some(Ordering::Less) => {
                *current = new.clone();
                return;
            }
            Some(Ordering::Greater) => return,
            _ => {} // types are different need to check slot
        }
    }

    if new.slot_updated > current.slot_updated {
        *current = new.clone();
    }
}

pub(crate) fn update_optional_field<T: Clone + Default>(
    current: &mut Option<Updated<T>>,
    new: &Option<Updated<T>>,
) {
    if current.clone().unwrap_or_default().update_version.is_some()
        && new.clone().unwrap_or_default().update_version.is_some()
    {
        match current
            .clone()
            .unwrap_or_default()
            .update_version
            .unwrap()
            .partial_cmp(&new.clone().unwrap_or_default().update_version.unwrap())
        {
            Some(Ordering::Less) => {
                *current = new.clone();
                return;
            }
            Some(Ordering::Greater) => return,
            _ => {} // types are different need to check slot
        }
    }

    if new.clone().unwrap_or_default().slot_updated
        > current.clone().unwrap_or_default().slot_updated
    {
        *current = new.clone();
    }
}

impl TypedColumn for AssetStaticDetails {
    type KeyType = Pubkey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "ASSET_STATIC_V2"; // Name of the column family

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetStaticDetailsDeprecated {
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

impl TypedColumn for AssetDynamicDetailsDeprecated {
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

impl TypedColumn for AssetDynamicDetails {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_DYNAMIC_V2";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for MetadataMintMap {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "METADATA_MINT_MAP";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}

impl TypedColumn for AssetAuthorityDeprecated {
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

impl TypedColumn for AssetAuthority {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_AUTHORITY_V2";

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
    pub fn merge_keep_existing(
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
                        update_field(&mut current_val.url, &new_val.url);
                        update_optional_field(
                            &mut current_val.chain_mutability,
                            &new_val.chain_mutability,
                        );
                        update_optional_field(&mut current_val.lamports, &new_val.lamports);
                        update_optional_field(&mut current_val.executable, &new_val.executable);
                        update_optional_field(
                            &mut current_val.metadata_owner,
                            &new_val.metadata_owner,
                        );
                        update_optional_field(&mut current_val.raw_name, &new_val.raw_name);
                        update_optional_field(
                            &mut current_val.mpl_core_plugins,
                            &new_val.mpl_core_plugins,
                        );
                        update_optional_field(
                            &mut current_val.mpl_core_unknown_plugins,
                            &new_val.mpl_core_unknown_plugins,
                        );
                        update_optional_field(&mut current_val.num_minted, &new_val.num_minted);
                        update_optional_field(&mut current_val.current_size, &new_val.current_size);
                        update_optional_field(&mut current_val.rent_epoch, &new_val.rent_epoch);
                        update_optional_field(
                            &mut current_val.plugins_json_version,
                            &new_val.plugins_json_version,
                        );
                        update_optional_field(
                            &mut current_val.mpl_core_external_plugins,
                            &new_val.mpl_core_external_plugins,
                        );
                        update_optional_field(
                            &mut current_val.mpl_core_unknown_external_plugins,
                            &new_val.mpl_core_unknown_external_plugins,
                        );

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
            self.chain_mutability
                .clone()
                .map_or(0, |onchain_data| onchain_data.slot_updated),
            self.lamports
                .clone()
                .map_or(0, |onchain_data| onchain_data.slot_updated),
            self.executable
                .clone()
                .map_or(0, |onchain_data| onchain_data.slot_updated),
            self.metadata_owner
                .clone()
                .map_or(0, |onchain_data| onchain_data.slot_updated),
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
        let mut write_version = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetAuthority>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    write_version = value.write_version;
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
                    if write_version.is_some() && new_val.write_version.is_some() {
                        if new_val.write_version.unwrap() > write_version.unwrap() {
                            slot = new_val.slot_updated;
                            write_version = new_val.write_version;
                            result = op.to_vec();
                        }
                    } else if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        write_version = new_val.write_version;
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

impl TypedColumn for AssetOwnerDeprecated {
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

impl TypedColumn for AssetOwner {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_OWNER_v2";

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
                        update_field(
                            &mut current_val.owner_delegate_seq,
                            &new_val.owner_delegate_seq,
                        );
                        update_field(&mut current_val.delegate, &new_val.delegate);

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
            self.delegate.slot_updated,
            self.owner_type.slot_updated,
            self.owner_delegate_seq.slot_updated,
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

impl TypedColumn for AssetCollectionDeprecated {
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

impl TypedColumn for AssetCollection {
    type KeyType = Pubkey;
    type ValueType = Self;
    const NAME: &'static str = "ASSET_COLLECTION_v2";

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
        let mut result: Option<Self> = None;
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(value) => {
                    result = Some(value);
                }
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    result = Some(if let Some(mut current_val) = result {
                        update_field(&mut current_val.collection, &new_val.collection);
                        update_field(
                            &mut current_val.is_collection_verified,
                            &new_val.is_collection_verified,
                        );
                        update_field(&mut current_val.authority, &new_val.authority);

                        current_val
                    } else {
                        new_val
                    });
                }
                Err(e) => {
                    error!("RocksDB: AssetCollection deserialize new_val: {}", e)
                }
            }
        }

        result.and_then(|result| serialize(&result).ok())
    }

    pub fn get_slot_updated(&self) -> u64 {
        [
            self.collection.slot_updated,
            self.is_collection_verified.slot_updated,
            self.authority.slot_updated,
        ]
        .into_iter()
        .max()
        .unwrap() // unwrap here is safe, because vec is not empty
    }

    pub fn get_seq(&self) -> Option<u64> {
        let seq_1 = match self.authority.update_version {
            Some(UpdateVersion::Sequence(seq)) => Some(seq),
            _ => None,
        };
        let seq_2 = match self.collection.update_version {
            Some(UpdateVersion::Sequence(seq)) => Some(seq),
            _ => None,
        };
        let seq_3 = match self.is_collection_verified.update_version {
            Some(UpdateVersion::Sequence(seq)) => Some(seq),
            _ => None,
        };
        [seq_1, seq_2, seq_3].into_iter().flatten().max()
    }
}

// AssetsUpdateIdx is a column family that is used to query the assets that were updated in (or after) a particular slot.
// The key is a concatenation of the slot and the asset pubkey.
// This will be used in the batch updater to the secondary index database. The batches should be constructed based on the slot, with an overlap of 1 slot including the last processed slot.
#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlotAssetIdx {}

impl TypedColumn for SlotAssetIdx {
    type KeyType = SlotAssetIdxKey;
    type ValueType = Self;
    const NAME: &'static str = "SLOT_ASSET_IDX";

    fn encode_key(key: SlotAssetIdxKey) -> Vec<u8> {
        key.encode_to_bytes()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        SlotAssetIdxKey::decode_from_bypes(bytes)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SlotAssetIdxKey {
    pub slot: u64,
    pub pubkey: Pubkey,
}

impl SlotAssetIdxKey {
    pub fn new(slot: u64, asset: Pubkey) -> SlotAssetIdxKey {
        SlotAssetIdxKey {
            slot,
            pubkey: asset,
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        encode_u64_pubkey(self.slot, self.pubkey)
    }

    pub fn decode_from_bypes(bytes: Vec<u8>) -> Result<SlotAssetIdxKey> {
        decode_u64_pubkey(bytes).map(|(slot, asset)| SlotAssetIdxKey {
            slot,
            pubkey: asset,
        })
    }
}
