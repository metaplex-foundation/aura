use serde::{Deserialize, Serialize};
use sqlx::FromRow;
#[derive(Serialize, Deserialize, Debug, Copy, Clone, sqlx::Type, PartialEq)]
#[sqlx(type_name = "royalty_target_type", rename_all = "snake_case")]
pub enum RoyaltyTargetType {
    Unknown,
    Creators,
    Fanout,
    Single,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, sqlx::Type, PartialEq)]
#[sqlx(type_name = "specification_asset_class", rename_all = "snake_case")]
pub enum SpecificationAssetClass {
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

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, sqlx::Type)]
#[sqlx(type_name = "specification_versions", rename_all = "snake_case")]
pub enum SpecificationVersions {
    Unknown,
    V0,
    V1,
    V2,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, sqlx::Type)]
#[sqlx(type_name = "owner_type", rename_all = "snake_case")]
pub enum OwnerType {
    Unknown,
    Token,
    Single,
}

// Structure to fetch the last synced key
#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct LastSyncedKey {
    last_synced_asset_update_key: Vec<u8>,
}

#[derive(Debug)]
pub struct AssetSortedIndex {
    pub pubkey: Vec<u8>,
    pub sorting_id: String,
}

#[derive(Default)]
pub struct SearchAssetsFilter {
    pub specification_version: Option<SpecificationVersions>,
    pub specification_asset_class: Option<SpecificationAssetClass>,
    pub owner_address: Option<Vec<u8>>,
    pub owner_type: Option<OwnerType>,
    pub creator_address: Option<Vec<u8>>,
    pub creator_verified: Option<bool>,
    pub authority_address: Option<Vec<u8>>,
    pub collection: Option<Vec<u8>>,
    pub delegate: Option<Vec<u8>>,
    pub frozen: Option<bool>,
    pub supply: Option<u64>,
    pub supply_mint: Option<Vec<u8>>,
    pub compressed: Option<bool>,
    pub compressible: Option<bool>,
    pub royalty_target_type: Option<RoyaltyTargetType>,
    pub royalty_target: Option<Vec<u8>>,
    pub royalty_amount: Option<u32>,
    pub burnt: Option<bool>,
    pub json_uri: Option<String>,
}

pub struct AssetSorting {
    pub sort_by: AssetSortBy,
    pub sort_direction: AssetSortDirection,
}

pub enum AssetSortBy {
    SlotCreated,
    SlotUpdated,
}

pub enum AssetSortDirection {
    Asc,
    Desc,
}
