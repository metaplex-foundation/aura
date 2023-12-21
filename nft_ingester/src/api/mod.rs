use open_rpc_schema::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use api_impl::*;
use digital_asset_types::rpc::{filter::AssetSorting, response::GetGroupingResponse};

use crate::api::error::DasApiError;

use self::validation::{validate_opt_pubkey, validate_pubkey};

pub mod api_impl;
pub mod builder;
pub mod config;
pub mod error;
pub mod middleware;
pub mod service;
pub mod validation;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByGroup {
    pub group_key: String,
    pub group_value: String,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByOwner {
    pub owner_address: String,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAsset {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetBatch {
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetProof {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetProofBatch {
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByCreator {
    pub creator_address: String,
    pub only_verified: Option<bool>,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByAuthority {
    pub authority_address: String,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetGrouping {
    pub group_key: String,
    pub group_value: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SearchAssets {
    // todo: negate and condition_type are not used in the current implementation
    pub negate: Option<bool>,
    pub condition_type: Option<digital_asset_types::rpc::filter::SearchConditionType>,
    pub interface: Option<digital_asset_types::rpc::Interface>,
    pub owner_address: Option<String>,
    pub owner_type: Option<digital_asset_types::rpc::OwnershipModel>,
    pub creator_address: Option<String>,
    pub creator_verified: Option<bool>,
    pub authority_address: Option<String>,
    pub grouping: Option<(String, String)>,
    pub delegate: Option<String>,
    pub frozen: Option<bool>,
    pub supply: Option<u64>,
    pub supply_mint: Option<String>,
    pub compressed: Option<bool>,
    pub compressible: Option<bool>,
    pub royalty_target_type: Option<digital_asset_types::rpc::RoyaltyModel>,
    pub royalty_target: Option<String>,
    pub royalty_amount: Option<u32>,
    pub burnt: Option<bool>,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    // before and after are base64 encoded sorting keys, which is NOT a pubkey, passing any non empty base64 encoded string will not cause an error
    pub before: Option<String>,
    pub after: Option<String>,
    #[serde(default)]
    pub json_uri: Option<String>,
    // todo: skipping displayOptions for now
    // #[serde(default, alias = "displayOptions")]
    // pub options: Option<Options>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

impl SearchAssets {
    pub fn extract_some_fields(&self) -> String {
        let mut result = String::new();
        fn check_and_append<T>(current: &mut String, field: &Option<T>, field_name: &str) {
            if field.is_some() {
                current.push_str(&format!("{}_", field_name));
            }
        }

        check_and_append(&mut result, &self.negate, "negate");
        check_and_append(&mut result, &self.condition_type, "condition_type");
        check_and_append(&mut result, &self.interface, "interface");
        check_and_append(&mut result, &self.owner_address, "owner_address");
        check_and_append(&mut result, &self.owner_type, "owner_type");
        check_and_append(&mut result, &self.creator_address, "creator_address");
        check_and_append(&mut result, &self.creator_verified, "creator_verified");
        check_and_append(&mut result, &self.authority_address, "authority_address");
        check_and_append(&mut result, &self.grouping, "grouping");
        check_and_append(&mut result, &self.delegate, "delegate");
        check_and_append(&mut result, &self.frozen, "frozen");
        check_and_append(&mut result, &self.supply, "supply");
        check_and_append(&mut result, &self.supply_mint, "supply_mint");
        check_and_append(&mut result, &self.compressed, "compressed");
        check_and_append(&mut result, &self.compressible, "compressible");
        check_and_append(
            &mut result,
            &self.royalty_target_type,
            "royalty_target_type",
        );
        check_and_append(&mut result, &self.royalty_target, "royalty_target");
        check_and_append(&mut result, &self.royalty_amount, "royalty_amount");
        check_and_append(&mut result, &self.burnt, "burnt");
        check_and_append(&mut result, &self.sort_by, "sort_by");
        check_and_append(&mut result, &self.limit, "limit");
        check_and_append(&mut result, &self.page, "page");
        check_and_append(&mut result, &self.before, "before");
        check_and_append(&mut result, &self.after, "after");
        check_and_append(&mut result, &self.json_uri, "json_uri");
        check_and_append(&mut result, &self.cursor, "cursor");
        check_and_append(&mut result, &self.name, "name");

        if result.is_empty() {
            return "no_filters".to_string();
        }
        result[..result.len() - 1].to_string()
    }
}

impl TryFrom<SearchAssets> for digital_asset_types::dao::SearchAssetsQuery {
    type Error = DasApiError;
    fn try_from(search_assets: SearchAssets) -> Result<Self, Self::Error> {
        let grouping = search_assets
            .grouping
            .map(|(key, val)| {
                if key != "collection" {
                    return Err(DasApiError::InvalidGroupingKey(key));
                }
                validate_pubkey(val).map(|pubkey| (key, pubkey.to_bytes().to_vec()))
            })
            .transpose()?;
        Ok(digital_asset_types::dao::SearchAssetsQuery {
            negate: search_assets.negate,
            condition_type: search_assets.condition_type.map(|s| s.into()),
            owner_address: validate_opt_pubkey(&search_assets.owner_address)?,
            owner_type: search_assets.owner_type.map(|s| s.into()),
            creator_address: validate_opt_pubkey(&search_assets.creator_address)?,
            creator_verified: search_assets.creator_verified,
            authority_address: validate_opt_pubkey(&search_assets.authority_address)?,
            grouping,
            delegate: validate_opt_pubkey(&search_assets.delegate)?,
            frozen: search_assets.frozen,
            supply: search_assets.supply,
            supply_mint: validate_opt_pubkey(&search_assets.supply_mint)?,
            compressed: search_assets.compressed,
            compressible: search_assets.compressible,
            royalty_target_type: search_assets.royalty_target_type.map(|s| s.into()),
            royalty_target: validate_opt_pubkey(&search_assets.royalty_target)?,
            royalty_amount: search_assets.royalty_amount,
            burnt: search_assets.burnt,
            json_uri: search_assets.json_uri,
            specification_version: search_assets.interface.as_ref().map(|s| s.into()),
            specification_asset_class: search_assets.interface.as_ref().map(|s| s.into()).filter(|v| v != &digital_asset_types::dao::sea_orm_active_enums::SpecificationAssetClass::Unknown),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_some_field_names_with_default_structure() {
        let search_assets = SearchAssets::default();

        let extracted_fields = search_assets.extract_some_fields();

        assert_eq!(extracted_fields, "no_filters".to_string());
    }

    #[test]
    fn extract_some_field_names_with_partially_filled_structure() {
        let search_assets = SearchAssets {
            burnt: Some(false),
            negate: Some(true),
            ..Default::default()
        };

        let extracted_fields = search_assets.extract_some_fields();

        assert_eq!(extracted_fields, "negate_burnt".to_string());
    }
}
