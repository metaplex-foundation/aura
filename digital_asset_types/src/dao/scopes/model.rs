use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssetAuthorityModel {
    pub id: i64,
    pub asset_id: Vec<u8>,
    pub scopes: Option<String>,
    pub authority: Vec<u8>,
    pub seq: i64,
    pub slot_updated: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssetCreatorsModel {
    pub id: i64,
    pub asset_id: Vec<u8>,
    pub creator: Vec<u8>,
    pub share: i32,
    pub verified: bool,
    pub seq: Option<i64>,
    pub slot_updated: Option<i64>,
    pub position: i16,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssetDataModel {
    pub id: Vec<u8>,
    pub chain_data_mutability: ChainMutability,
    pub chain_data: Json,
    pub metadata_url: String,
    pub metadata_mutability: Mutability,
    pub metadata: Json,
    pub slot_updated: i64,
    pub reindex: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetGroupingModel {
    pub id: i64,
    pub asset_id: Vec<u8>,
    pub group_key: String,
    pub group_value: Option<String>,
    pub seq: Option<i64>,
    pub slot_updated: Option<i64>,
    pub verified: Option<bool>,
    pub group_info_seq: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssetModel {
    pub id: Vec<u8>,
    pub alt_id: Option<Vec<u8>>,
    pub specification_version: Option<SpecificationVersions>,
    pub specification_asset_class: Option<SpecificationAssetClass>,
    pub owner: Option<Vec<u8>>,
    pub owner_type: OwnerType,
    pub delegate: Option<Vec<u8>>,
    pub frozen: bool,
    pub supply: i64,
    pub supply_mint: Option<Vec<u8>>,
    pub compressed: bool,
    pub compressible: bool,
    pub seq: Option<i64>,
    pub tree_id: Option<Vec<u8>>,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<i64>,
    pub royalty_target_type: RoyaltyTargetType,
    pub royalty_target: Option<Vec<u8>>,
    pub royalty_amount: i32,
    pub asset_data: Option<Vec<u8>>,
    pub created_at: Option<i64>,
    pub burnt: bool,
    pub slot_updated: Option<i64>,
    pub data_hash: Option<String>,
    pub creator_hash: Option<String>,
    pub owner_delegate_seq: Option<i64>,
    pub was_decompressed: bool,
    pub leaf_seq: Option<i64>,
    pub plugins: Option<serde_json::Value>,
    pub unknown_plugins: Option<serde_json::Value>,
    pub num_minted: Option<u32>,
    pub current_supply: Option<u32>,
    pub plugins_json_version: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClItemsModel {
    pub id: i64,
    pub tree: Vec<u8>,
    pub node_idx: i64,
    pub leaf_idx: Option<i64>,
    pub seq: i64,
    pub level: i64,
    pub hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Mutability {
    Immutable,
    Mutable,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TaskStatus {
    Failed,
    Pending,
    Running,
    Success,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RoyaltyTargetType {
    Creators,
    Fanout,
    Single,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SpecificationAssetClass {
    FungibleAsset,
    FungibleToken,
    IdentityNft,
    Nft,
    NonTransferableNft,
    Print,
    PrintableNft,
    ProgrammableNft,
    TransferRestrictedNft,
    MplCoreAsset,
    MplCoreCollection,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChainMutability {
    Immutable,
    Mutable,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SpecificationVersions {
    Unknown,
    V0,
    V1,
    V2,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OwnerType {
    Single,
    Token,
    Unknown,
}
