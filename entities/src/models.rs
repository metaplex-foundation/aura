use crate::enums::{
    OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions, TokenStandard,
    UseMethod,
};
use serde::{Deserialize, Serialize};
use solana_sdk::{hash::Hash, pubkey::Pubkey};

// AssetIndex is the struct that is stored in the postgres database and is used to query the asset pubkeys.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct AssetIndex {
    // immutable fields
    pub pubkey: Pubkey,
    pub specification_version: SpecificationVersions,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub slot_created: i64,
    // mutable fields
    pub owner_type: Option<OwnerType>,
    pub owner: Option<Pubkey>,
    pub delegate: Option<Pubkey>,
    pub authority: Option<Pubkey>,
    pub collection: Option<Pubkey>,
    pub is_collection_verified: Option<bool>,
    pub creators: Vec<Creator>,
    pub royalty_amount: i64,
    pub is_burnt: bool,
    pub is_compressible: bool,
    pub is_compressed: bool,
    pub is_frozen: bool,
    pub supply: i64,
    pub metadata_url: Option<String>,
    pub slot_updated: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Creator {
    pub creator: Pubkey,
    pub creator_verified: bool,
    // In percentages, NOT basis points
    pub creator_share: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompleteAssetDetails {
    // From AssetStaticDetails
    pub pubkey: Pubkey,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub created_at: i64,

    // From AssetDynamicDetails as Tuples
    pub is_compressible: (bool, u64, Option<u64>),
    pub is_compressed: (bool, u64, Option<u64>),
    pub is_frozen: (bool, u64, Option<u64>),
    pub supply: (Option<u64>, u64, Option<u64>),
    pub seq: (Option<u64>, u64, Option<u64>),
    pub is_burnt: (bool, u64, Option<u64>),
    pub was_decompressed: (bool, u64, Option<u64>),
    pub onchain_data: (Option<ChainDataV1>, u64, Option<u64>), // Serialized ChainDataV1
    pub creators: (Vec<Creator>, u64, Option<u64>),
    pub royalty_amount: (u16, u64, Option<u64>),

    // From AssetAuthority as Tuple
    pub authority: (Pubkey, u64, Option<u64>),

    // From AssetOwner as Tuples
    pub owner: (Pubkey, u64, Option<u64>),
    pub delegate: (Option<Pubkey>, u64, Option<u64>),
    pub owner_type: (OwnerType, u64, Option<u64>),
    pub owner_delegate_seq: (Option<u64>, u64, Option<u64>),

    // Separate fields
    pub leaves: Vec<AssetLeaf>,
    pub collection: AssetCollection,
}

/// Leaf information about compressed asset
/// Nonce - is basically the leaf index. It takes from tree supply.
/// NOTE: leaf index is not the same as node index. Leaf index is specifically the index of the leaf in the tree.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetLeaf {
    pub tree_id: Pubkey,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<u64>,
    pub data_hash: Option<Hash>,
    pub creator_hash: Option<Hash>,
    pub leaf_seq: Option<u64>,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Uses {
    pub use_method: UseMethod,
    pub remaining: u64,
    pub total: u64,
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
        self.name = self.name.trim().replace('\0', "");
        self.symbol = self.symbol.trim().replace('\0', "");
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetCollection {
    pub collection: Pubkey,
    pub is_collection_verified: bool,
    pub collection_seq: Option<u64>,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct Updated<T> {
    pub slot_updated: u64,
    pub seq: Option<u64>,
    pub value: T,
}

impl<T> Updated<T> {
    pub fn new(slot_updated: u64, seq: Option<u64>, value: T) -> Self {
        Self {
            slot_updated,
            seq,
            value,
        }
    }
}
