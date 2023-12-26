use crate::key_encoders::{decode_pubkey, decode_u64x2_pubkey, encode_pubkey, encode_u64x2_pubkey};
use crate::Result;
use bincode::deserialize;
use blockbuster::token_metadata::state::{TokenStandard, Uses};
use log::{error, warn};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_sdk::{hash::Hash, pubkey::Pubkey};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Default)]
pub enum RoyaltyTargetType {
    #[default]
    Unknown,
    Creators,
    Fanout,
    Single,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Default)]
pub enum SpecificationVersions {
    #[default]
    Unknown,
    V0,
    V1,
    V2,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Default)]
pub enum SpecificationAssetClass {
    #[default]
    Unknown,
    FungibleToken,
    FungibleAsset,
    Nft,
    PrintableNft,
    ProgrammableNft,
    Print,
    TransferRestrictedNft,
    NonTransferableNft,
    IdentityNft,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub enum OwnerType {
    Unknown,
    Token,
    Single,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetDynamicDetails {
    pub pubkey: Pubkey,
    pub is_compressible: bool,
    pub is_compressed: bool,
    pub is_frozen: bool,
    pub supply: Option<u64>,
    pub seq: Option<u64>,
    pub is_burnt: bool,
    pub was_decompressed: bool,
    pub onchain_data: Option<String>,
    pub creators: Vec<Creator>,
    pub royalty_amount: u16,
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

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct AssetLeaf {
    pub pubkey: Pubkey,
    pub tree_id: Pubkey,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<u32>,
    pub data_hash: Option<Hash>,
    pub creator_hash: Option<Hash>,
    pub leaf_seq: Option<u64>,
    pub slot_updated: u64,
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
            match deserialize::<AssetLeaf>(&op) {
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
                    } else {
                        if new_val.slot_updated > slot {
                            slot = new_val.slot_updated;
                            result = op.to_vec();
                        }
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Creator {
    pub creator: Pubkey,
    pub creator_verified: bool,
    // In percentages, NOT basis points
    pub creator_share: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChainDataV1 {
    pub name: String,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edition_nonce: Option<u8>,
    pub primary_sale_happened: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_standard: Option<TokenStandard>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uses: Option<Uses>,
}

impl ChainDataV1 {
    pub fn sanitize(&mut self) {
        self.name = self.name.trim().replace("\0", "").to_string();
        self.symbol = self.symbol.trim().replace("\0", "").to_string();
    }
}

use crate::TypedColumn;

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

impl AssetDynamicDetails {
    pub fn merge_dynamic_details(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        if let Some(existing_val) = existing_val {
            match deserialize::<AssetDynamicDetails>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    result = existing_val.to_vec();
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
            match deserialize::<AssetDynamicDetails>(&op) {
                Ok(new_val) => {
                    if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: AssetDynamicDetails deserialize new_val: {}", e)
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
            match deserialize::<AssetOwner>(&op) {
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
                    } else {
                        if new_val.slot_updated > slot {
                            slot = new_val.slot_updated;
                            result = op.to_vec();
                        }
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
