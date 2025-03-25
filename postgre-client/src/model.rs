use std::{
    fmt,
    fmt::{Display, Formatter},
};

use entities::enums::TokenType;
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
    ProgrammableNft,
    MplCoreAsset,
    MplCoreCollection,
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

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, sqlx::Type)]
#[sqlx(type_name = "batch_mint_state", rename_all = "snake_case")]
pub enum BatchMintState {
    Uploaded,
    ValidationFail,
    ValidationComplete,
    UploadedToArweave,
    FailUploadToArweave,
    FailSendingTransaction,
    Complete,
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

#[derive(Default, Debug)]
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
    pub supply: Option<AssetSupply>,
    pub supply_mint: Option<Vec<u8>>,
    pub compressed: Option<bool>,
    pub compressible: Option<bool>,
    pub royalty_target_type: Option<RoyaltyTargetType>,
    pub royalty_target: Option<Vec<u8>>,
    pub royalty_amount: Option<u32>,
    pub burnt: Option<bool>,
    pub json_uri: Option<String>,
    pub token_type: Option<TokenType>,
}

#[derive(Debug)]
pub enum AssetSupply {
    Greater(u64),
    Equal(u64),
}

pub struct AssetSorting {
    pub sort_by: AssetSortBy,
    pub sort_direction: AssetSortDirection,
}

// As a value for enum variants DB column used
pub enum AssetSortBy {
    SlotCreated,
    SlotUpdated,
    Key,
}

impl Display for AssetSortBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssetSortBy::SlotCreated => write!(f, "ast_slot_created"),
            AssetSortBy::SlotUpdated => write!(f, "ast_slot_updated"),
            AssetSortBy::Key => write!(f, "ast_pubkey"),
        }
    }
}

pub enum AssetSortDirection {
    Asc,
    Desc,
}

impl From<entities::api_req_params::AssetSorting> for AssetSorting {
    fn from(sorting: entities::api_req_params::AssetSorting) -> Self {
        Self {
            sort_by: sorting.sort_by.into(),
            sort_direction: sorting.sort_direction.map_or(AssetSortDirection::Desc, |v| v.into()),
        }
    }
}

impl From<entities::api_req_params::AssetSortBy> for AssetSortBy {
    fn from(sort_by: entities::api_req_params::AssetSortBy) -> Self {
        match sort_by {
            entities::api_req_params::AssetSortBy::Created => Self::SlotCreated,
            entities::api_req_params::AssetSortBy::RecentAction
            | entities::api_req_params::AssetSortBy::Updated => Self::SlotUpdated,
            _ => Self::Key,
        }
    }
}

impl From<entities::api_req_params::AssetSortDirection> for AssetSortDirection {
    fn from(sort_direction: entities::api_req_params::AssetSortDirection) -> Self {
        match sort_direction {
            entities::api_req_params::AssetSortDirection::Asc => Self::Asc,
            entities::api_req_params::AssetSortDirection::Desc => Self::Desc,
        }
    }
}

pub(crate) enum VerificationRequiredField {
    Owner,
    Authority,
    Group,
}

impl Display for VerificationRequiredField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let text = match self {
            VerificationRequiredField::Owner => "ast_owner",
            VerificationRequiredField::Authority => "ast_authority",
            VerificationRequiredField::Group => "ast_collection",
        };
        write!(f, "{}", text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_verification_required_field() {
        assert_eq!(VerificationRequiredField::Owner.to_string(), "ast_owner");
        assert_eq!(VerificationRequiredField::Authority.to_string(), "ast_authority");
        assert_eq!(VerificationRequiredField::Group.to_string(), "ast_collection");
    }
}
