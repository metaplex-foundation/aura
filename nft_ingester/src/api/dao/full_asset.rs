use crate::api::dao::scopes::model;
use entities::models::{EditionData, OffChainData};
use rocks_db::asset::{AssetCollection, AssetLeaf};
use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails};
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct FullAsset {
    pub asset_static: AssetStaticDetails,
    pub asset_owner: AssetOwner,
    pub asset_dynamic: AssetDynamicDetails,
    pub asset_leaf: AssetLeaf,
    pub offchain_data: OffChainData,
    pub asset_collections: HashMap<Pubkey, AssetCollection>,
    pub assets_authority: HashMap<Pubkey, AssetAuthority>,
    pub edition_data: Option<EditionData>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct AssetRelated {
    pub authorities: Vec<model::AssetAuthorityModel>,
    pub creators: Vec<model::AssetCreatorsModel>,
    pub groups: Vec<model::AssetGroupingModel>,
}

pub struct FullAssetList {
    pub list: Vec<FullAsset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AssetDataModel {
    pub asset: model::AssetDataModel,
    pub lamports: Option<u64>,
    pub rent_epoch: Option<u64>,
    pub executable: Option<bool>,
    pub metadata_owner: Option<String>,
}
