use crate::dao::scopes::model;
use entities::models::EditionData;

#[derive(Clone, Debug, PartialEq)]
pub struct FullAsset {
    pub asset: model::AssetModel,
    pub data: AssetDataModel,
    pub authorities: Vec<model::AssetAuthorityModel>,
    pub creators: Vec<model::AssetCreatorsModel>,
    pub groups: Vec<model::AssetGroupingModel>,
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
