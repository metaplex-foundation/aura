use entities::api_req_params::Options;
use interface::json::{JsonDownloader, JsonPersister};
use interface::price_fetcher::TokenPriceFetcher;
use metrics_utils::ApiMetricsConfig;
use rocks_db::{errors::StorageError, Storage};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};

use crate::api::dapi::asset;
use crate::api::dapi::rpc_asset_convertors::asset_to_rpc;
use crate::api::dapi::rpc_asset_models::Asset;

use super::asset_preview::populate_previews_opt;

#[allow(clippy::too_many_arguments)]
pub async fn get_asset_batch<TPF: TokenPriceFetcher>(
    rocks_db: Arc<Storage>,
    ids: Vec<Pubkey>,
    options: Options,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    storage_service_base_path: Option<String>,
    token_price_fetcher: Arc<TPF>,
    metrics: Arc<ApiMetricsConfig>,
) -> Result<Vec<Option<Asset>>, StorageError> {
    let assets = asset::get_by_ids(
        rocks_db.clone(),
        ids,
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
        tasks,
        &None,
        token_price_fetcher,
        metrics,
    )
    .await?;

    let mut result = assets
        .into_iter()
        .map(|asset| match asset {
            Some(asset) => Ok(Some(asset_to_rpc(asset, &None)?.unwrap())),
            None => Ok(None),
        })
        .collect::<Result<Vec<Option<Asset>>, StorageError>>()?;

    if let Some(base_url) = storage_service_base_path.as_ref() {
        let _ = populate_previews_opt(base_url, &rocks_db, &mut result).await;
    }

    Ok(result)
}
