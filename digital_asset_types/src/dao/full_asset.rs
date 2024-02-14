use crate::dao::{asset, asset_authority, asset_creators, asset_data, asset_grouping};
use entities::models::EditionData;

#[derive(Clone, Debug, PartialEq)]
pub struct FullAsset {
    pub asset: asset::Model,
    pub data: AssetDataModel,
    pub authorities: Vec<asset_authority::Model>,
    pub creators: Vec<asset_creators::Model>,
    pub groups: Vec<asset_grouping::Model>,
    pub edition_data: Option<EditionData>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct AssetRelated {
    pub authorities: Vec<asset_authority::Model>,
    pub creators: Vec<asset_creators::Model>,
    pub groups: Vec<asset_grouping::Model>,
}

pub struct FullAssetList {
    pub list: Vec<FullAsset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AssetDataModel {
    pub asset: asset_data::Model,
    pub lamports: Option<u64>,
    pub executable: Option<bool>,
    pub metadata_owner: Option<String>,
}
