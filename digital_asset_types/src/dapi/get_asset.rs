use std::sync::Arc;

use entities::api_req_params::Options;
use interface::json::JsonProcessor;
use sea_orm::DbErr;
use solana_sdk::pubkey::Pubkey;

use rocks_db::Storage;

use crate::{dao::scopes, rpc::Asset};

use super::common::asset_to_rpc;

pub async fn get_asset(
    rocks_db: Arc<Storage>,
    id: Pubkey,
    options: Options,
    json_downloader: Option<Arc<impl JsonProcessor + Sync + Send + 'static>>,
    persist_json: bool,
    max_json_to_download: usize,
) -> Result<Option<Asset>, DbErr> {
    let assets = scopes::asset::get_by_ids(
        rocks_db,
        vec![id],
        options,
        json_downloader,
        persist_json,
        max_json_to_download,
    )
    .await?;

    match &assets[0] {
        Some(asset) => asset_to_rpc(asset.clone()),
        None => Ok(None),
    }
}
