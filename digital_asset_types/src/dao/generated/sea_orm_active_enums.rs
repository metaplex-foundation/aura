//! SeaORM Entity. Generated by sea-orm-codegen 0.9.3

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "mutability")]
pub enum Mutability {
    #[sea_orm(string_value = "immutable")]
    Immutable,
    #[sea_orm(string_value = "mutable")]
    Mutable,
    #[sea_orm(string_value = "unknown")]
    Unknown,
}
#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "v1_account_attachments"
)]
pub enum V1AccountAttachments {
    #[sea_orm(string_value = "edition")]
    Edition,
    #[sea_orm(string_value = "edition_marker")]
    EditionMarker,
    #[sea_orm(string_value = "master_edition_v1")]
    MasterEditionV1,
    #[sea_orm(string_value = "master_edition_v2")]
    MasterEditionV2,
    #[sea_orm(string_value = "unknown")]
    Unknown,
}
#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "task_status")]
pub enum TaskStatus {
    #[sea_orm(string_value = "failed")]
    Failed,
    #[sea_orm(string_value = "pending")]
    Pending,
    #[sea_orm(string_value = "running")]
    Running,
    #[sea_orm(string_value = "success")]
    Success,
}
#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "royalty_target_type"
)]
pub enum RoyaltyTargetType {
    #[sea_orm(string_value = "creators")]
    Creators,
    #[sea_orm(string_value = "fanout")]
    Fanout,
    #[sea_orm(string_value = "single")]
    Single,
    #[sea_orm(string_value = "unknown")]
    Unknown,
}
#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "specification_asset_class"
)]
pub enum SpecificationAssetClass {
    #[sea_orm(string_value = "fungible_asset")]
    FungibleAsset,
    #[sea_orm(string_value = "fungible_token")]
    FungibleToken,
    #[sea_orm(string_value = "identity_nft")]
    IdentityNft,
    #[sea_orm(string_value = "nft")]
    Nft,
    #[sea_orm(string_value = "non_transferable_nft")]
    NonTransferableNft,
    #[sea_orm(string_value = "print")]
    Print,
    #[sea_orm(string_value = "printable_nft")]
    PrintableNft,
    #[sea_orm(string_value = "programmable_nft")]
    ProgrammableNft,
    #[sea_orm(string_value = "transfer_restricted_nft")]
    TransferRestrictedNft,
    #[sea_orm(string_value = "mpl_core_asset")]
    MplCoreAsset,
    #[sea_orm(string_value = "mpl_core_collection")]
    MplCoreCollection,
    #[sea_orm(string_value = "unknown")]
    Unknown,
}
#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "chain_mutability")]
pub enum ChainMutability {
    #[sea_orm(string_value = "immutable")]
    Immutable,
    #[sea_orm(string_value = "mutable")]
    Mutable,
    #[sea_orm(string_value = "unknown")]
    Unknown,
}

impl From<entities::enums::ChainMutability> for ChainMutability {
    fn from(value: entities::enums::ChainMutability) -> Self {
        match value {
            entities::enums::ChainMutability::Immutable => ChainMutability::Immutable,
            entities::enums::ChainMutability::Mutable => ChainMutability::Mutable,
        }
    }
}
#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "specification_versions"
)]
pub enum SpecificationVersions {
    #[sea_orm(string_value = "unknown")]
    Unknown,
    #[sea_orm(string_value = "v0")]
    V0,
    #[sea_orm(string_value = "v1")]
    V1,
    #[sea_orm(string_value = "v2")]
    V2,
}
#[derive(Debug, Clone, PartialEq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "owner_type")]
pub enum OwnerType {
    #[sea_orm(string_value = "single")]
    Single,
    #[sea_orm(string_value = "token")]
    Token,
    #[sea_orm(string_value = "unknown")]
    Unknown,
}
