use std::sync::Arc;

use entities::api_req_params::Options;
use interface::json::{JsonDownloader, JsonPersister};
use interface::price_fetcher::TokenPriceFetcher;
use rocks_db::{errors::StorageError, Storage};
use solana_sdk::pubkey::Pubkey;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};

use crate::api::dapi::asset;
use crate::api::dapi::rpc_asset_convertors::asset_to_rpc;
use crate::api::dapi::rpc_asset_models::Asset;

use super::asset_preview::populate_previews_slice;

#[allow(clippy::too_many_arguments)]
pub async fn get_asset<TPF: TokenPriceFetcher>(
    rocks_db: Arc<Storage>,
    id: Pubkey,
    options: Options,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    storage_service_base_path: Option<String>,
    token_price_fetcher: Arc<TPF>,
) -> Result<Option<Asset>, StorageError> {
    let assets = asset::get_by_ids(
        rocks_db.clone(),
        vec![id],
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
        tasks,
        None,
        token_price_fetcher,
    )
    .await?;

    let mut result = match &assets[0] {
        Some(asset) => asset_to_rpc(asset.clone()),
        None => Ok(None),
    };

    if let Ok(Some(asset)) = &mut result {
        if let Some(base_url) = storage_service_base_path {
            let _ = populate_previews_slice(&base_url, &rocks_db, &mut [asset]).await;
        }
    }

    result
}
