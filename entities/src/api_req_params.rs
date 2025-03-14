use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::enums::{Interface, OwnershipModel, RoyaltyModel, TokenType};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssetSorting {
    pub sort_by: AssetSortBy,
    pub sort_direction: Option<AssetSortDirection>,
}

impl Default for AssetSorting {
    fn default() -> AssetSorting {
        AssetSorting {
            sort_by: AssetSortBy::Key,
            sort_direction: Some(AssetSortDirection::default()),
        }
    }
}

#[derive(Default)]
pub struct Pagination {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum AssetSortBy {
    #[serde(rename = "created")]
    Created,
    #[serde(rename = "updated")]
    Updated,
    #[serde(rename = "recent_action")]
    RecentAction,
    #[serde(rename = "key")]
    Key,
    #[serde(rename = "none")]
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, Default)]
pub enum AssetSortDirection {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    #[default]
    Desc,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Options {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_inscription: bool,
    // this option is present in displayOptions but in fact do not cause
    // any effect in reference API
    #[serde(default)]
    pub show_fungible: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SearchAssetsOptions {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_grand_total: bool,
    #[serde(default)]
    pub show_native_balance: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_inscription: bool,
    #[serde(default)]
    pub show_zero_balance: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetByMethodsOptions {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_grand_total: bool,
    #[serde(default)]
    pub show_native_balance: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_inscription: bool,
    #[serde(default)]
    pub show_zero_balance: bool,
    #[serde(default)]
    pub show_fungible: bool,
}

impl From<&SearchAssetsOptions> for Options {
    fn from(value: &SearchAssetsOptions) -> Self {
        Self {
            show_unverified_collections: value.show_unverified_collections,
            show_collection_metadata: value.show_collection_metadata,
            show_inscription: value.show_inscription,
            show_fungible: false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema)]
pub enum SearchConditionType {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "any")]
    Any,
}

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
    #[serde(default)]
    pub options: GetByMethodsOptions,
    pub cursor: Option<String>,
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
    #[serde(default)]
    pub options: GetByMethodsOptions,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAsset {
    pub id: String,
    #[serde(default)]
    pub options: Options,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetBatch {
    pub ids: Vec<String>,
    #[serde(default)]
    pub options: Options,
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
    #[serde(default)]
    pub options: GetByMethodsOptions,
    pub cursor: Option<String>,
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
    #[serde(default)]
    pub options: GetByMethodsOptions,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetGrouping {
    pub group_key: String,
    pub group_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetSignatures {
    pub id: Option<String>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    // before and after params for this method
    // represented as sequence numbers
    pub before: Option<String>,
    pub after: Option<String>,
    pub tree: Option<String>,
    pub leaf_index: Option<u64>,
    #[serde(default)]
    pub sort_direction: Option<AssetSortDirection>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DisplayOptions {
    pub show_zero_balance: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetTokenAccounts {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub owner: Option<String>,
    pub mint: Option<String>,
    #[serde(default, alias = "displayOptions")]
    pub options: Option<DisplayOptions>,
    pub before: Option<String>,
    pub after: Option<String>,
    pub cursor: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SearchAssets {
    // todo: negate and condition_type are not used in the current implementation
    pub negate: Option<bool>,
    pub condition_type: Option<SearchConditionType>,
    pub interface: Option<Interface>,
    pub owner_address: Option<String>,
    pub owner_type: Option<OwnershipModel>,
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
    pub royalty_target_type: Option<RoyaltyModel>,
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
    #[serde(default)]
    pub options: SearchAssetsOptions,
    #[serde(default)]
    pub token_type: Option<TokenType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCoreFees {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
    pub cursor: Option<String>,
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
        check_and_append(&mut result, &self.royalty_target_type, "royalty_target_type");
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
        check_and_append(&mut result, &self.token_type, "token_type");

        if result.is_empty() {
            return "no_filters".to_string();
        }
        result[..result.len() - 1].to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SearchAssetsV0 {
    pub negate: Option<bool>,
    pub condition_type: Option<SearchConditionType>,
    pub interface: Option<Interface>,
    pub owner_address: Option<String>,
    pub owner_type: Option<OwnershipModel>,
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
    pub royalty_target_type: Option<RoyaltyModel>,
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
}

impl From<SearchAssetsV0> for SearchAssets {
    fn from(value: SearchAssetsV0) -> Self {
        Self {
            negate: value.negate,
            condition_type: value.condition_type,
            interface: value.interface,
            owner_address: value.owner_address,
            owner_type: value.owner_type,
            creator_address: value.creator_address,
            creator_verified: value.creator_verified,
            authority_address: value.authority_address,
            grouping: value.grouping,
            delegate: value.delegate,
            frozen: value.frozen,
            supply: value.supply,
            supply_mint: value.supply_mint,
            compressed: value.compressed,
            compressible: value.compressible,
            royalty_target_type: value.royalty_target_type,
            royalty_target: value.royalty_target,
            royalty_amount: value.royalty_amount,
            token_type: None,
            burnt: value.burnt,
            sort_by: value.sort_by,
            limit: value.limit,
            page: value.page,
            before: value.before,
            after: value.after,
            json_uri: value.json_uri,
            cursor: None,
            name: None,
            options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetV0 {
    pub id: String,
}

impl From<GetAssetV0> for GetAsset {
    fn from(value: GetAssetV0) -> Self {
        Self { id: value.id, options: Default::default() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetBatchV0 {
    pub ids: Vec<String>,
}

impl From<GetAssetBatchV0> for GetAssetBatch {
    fn from(value: GetAssetBatchV0) -> Self {
        Self { ids: value.ids, options: Default::default() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByAuthorityV0 {
    pub authority_address: String,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl From<GetAssetsByAuthorityV0> for GetAssetsByAuthority {
    fn from(value: GetAssetsByAuthorityV0) -> Self {
        Self {
            authority_address: value.authority_address,
            sort_by: value.sort_by,
            limit: value.limit,
            page: value.page,
            before: value.before,
            after: value.after,
            cursor: None,
            options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByCreatorV0 {
    pub creator_address: String,
    pub only_verified: Option<bool>,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl From<GetAssetsByCreatorV0> for GetAssetsByCreator {
    fn from(value: GetAssetsByCreatorV0) -> Self {
        Self {
            creator_address: value.creator_address,
            only_verified: value.only_verified,
            sort_by: value.sort_by,
            limit: value.limit,
            page: value.page,
            before: value.before,
            after: value.after,
            cursor: None,
            options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByOwnerV0 {
    pub owner_address: String,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl From<GetAssetsByOwnerV0> for GetAssetsByOwner {
    fn from(value: GetAssetsByOwnerV0) -> Self {
        Self {
            owner_address: value.owner_address,
            sort_by: value.sort_by,
            limit: value.limit,
            page: value.page,
            before: value.before,
            after: value.after,
            cursor: None,
            options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetsByGroupV0 {
    pub group_key: String,
    pub group_value: String,
    pub sort_by: Option<AssetSorting>,
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl From<GetAssetsByGroupV0> for GetAssetsByGroup {
    fn from(value: GetAssetsByGroupV0) -> Self {
        Self {
            group_key: value.group_key,
            group_value: value.group_value,
            sort_by: value.sort_by,
            limit: value.limit,
            page: value.page,
            before: value.before,
            after: value.after,
            cursor: None,
            options: Default::default(),
        }
    }
}

impl From<SearchAssetsOptions> for GetByMethodsOptions {
    fn from(value: SearchAssetsOptions) -> Self {
        Self {
            show_unverified_collections: value.show_unverified_collections,
            show_grand_total: value.show_grand_total,
            show_native_balance: value.show_native_balance,
            show_collection_metadata: value.show_collection_metadata,
            show_inscription: value.show_inscription,
            show_zero_balance: value.show_zero_balance,
            show_fungible: false,
        }
    }
}

impl From<GetByMethodsOptions> for Options {
    fn from(value: GetByMethodsOptions) -> Self {
        Self {
            show_unverified_collections: value.show_unverified_collections,
            show_collection_metadata: value.show_collection_metadata,
            show_inscription: value.show_inscription,
            show_fungible: value.show_fungible,
        }
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
        let search_assets =
            SearchAssets { burnt: Some(false), negate: Some(true), ..Default::default() };

        let extracted_fields = search_assets.extract_some_fields();

        assert_eq!(extracted_fields, "negate_burnt".to_string());
    }
}
