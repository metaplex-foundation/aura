use entities::{
    api_req_params::Pagination,
    models::{AssetSignature, CoreFeesAccount, ResponseTokenAccount},
};
use rocks_db::columns::asset::{AssetEditionInfo, MasterAssetEditionsInfo};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::api::dapi::rpc_asset_models::Asset;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct AssetError {
    pub id: String,
    pub error: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct GetGroupingResponse {
    pub group_key: String,
    pub group_name: String,
    pub group_size: u64,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct HealthCheckResponse {
    pub status: Status,
    pub app_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    pub checks: Vec<Check>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_info: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Check {
    pub status: Status,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Check {
    pub fn new(name: String) -> Self {
        Self { status: Status::Ok, name, description: None }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Ok,
    Degraded,
    Unhealthy,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct NativeBalance {
    pub lamports: u64,
    pub price_per_sol: f64,
    pub total_price: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct InscriptionResponse {
    pub authority: String,
    pub content_type: String,
    pub encoding: String,
    pub inscription_data_account: String,
    pub order: u64,
    pub size: u32,
    pub validation_hash: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct AssetList {
    pub total: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grand_total: Option<u32>,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub items: Vec<Asset>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<AssetError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "nativeBalance")]
    pub native_balance: Option<NativeBalance>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
pub struct SignatureItem {
    pub signature: String,
    pub instruction: String,
    pub slot: u64,
}

impl From<AssetSignature> for SignatureItem {
    fn from(value: AssetSignature) -> Self {
        Self { signature: value.tx, instruction: value.instruction, slot: value.slot }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct MasterAssetEditionsInfoResponse {
    pub master_edition_address: String,
    pub supply: u64,
    pub max_supply: Option<u64>,
    pub editions: Vec<AssetEditionInfoResponse>,

    #[serde(flatten)]
    pub pagination: PaginationResponse,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
pub struct AssetEditionInfoResponse {
    pub mint: String,
    pub edition_address: String,
    pub edition: u64,
}
impl From<&AssetEditionInfo> for AssetEditionInfoResponse {
    fn from(value: &AssetEditionInfo) -> Self {
        Self {
            mint: value.mint.to_string(),
            edition_address: value.edition_address.to_string(),
            edition: value.edition,
        }
    }
}

impl From<(MasterAssetEditionsInfo, Pagination)> for MasterAssetEditionsInfoResponse {
    fn from((value, pagination): (MasterAssetEditionsInfo, Pagination)) -> Self {
        Self {
            master_edition_address: value.master_edition_address.to_string(),
            supply: value.supply,
            max_supply: value.max_supply,
            editions: value.editions.iter().map(AssetEditionInfoResponse::from).collect(),
            pagination: PaginationResponse {
                total: u32::try_from(value.editions.len()).ok().unwrap_or(u32::MAX),
                limit: pagination.limit.unwrap(),
                page: pagination.page,
                before: pagination.before,
                after: pagination.after,
                cursor: pagination.cursor,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct PaginationResponse {
    pub total: u32,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct TransactionSignatureList {
    pub total: u32,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub items: Vec<SignatureItem>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct TransactionSignatureListDeprecated {
    pub total: u32,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub items: Vec<(String, String)>,
}

impl From<TransactionSignatureList> for TransactionSignatureListDeprecated {
    fn from(value: TransactionSignatureList) -> Self {
        Self {
            total: value.total,
            limit: value.limit,
            page: value.page,
            before: value.before,
            after: value.after,
            items: value
                .items
                .into_iter()
                .map(|items| (items.signature, items.instruction))
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct TokenAccountsList {
    pub total: u32,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
    pub cursor: Option<String>,
    pub token_accounts: Vec<ResponseTokenAccount>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct CoreFeesAccountsList {
    pub total: u64,
    pub limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<u64>,
    pub before: Option<String>,
    pub after: Option<String>,
    pub cursor: Option<String>,
    pub core_fees_account: Vec<CoreFeesAccount>,
}
