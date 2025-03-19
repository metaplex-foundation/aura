use std::sync::Arc;

use entities::api_req_params::Options;
use interface::{
    json::{JsonDownloader, JsonPersister},
    price_fetcher::TokenPriceFetcher,
    processing_possibility::ProcessingPossibilityChecker,
};
use metrics_utils::ApiMetricsConfig;
use rocks_db::{errors::StorageError, Storage};
use solana_sdk::pubkey::Pubkey;

use super::asset_preview::populate_previews_slice;
use crate::api::dapi::{asset, rpc_asset_convertors::asset_to_rpc, rpc_asset_models::Asset};

#[allow(clippy::too_many_arguments)]
pub async fn get_asset<
    TPF: TokenPriceFetcher,
    JD: JsonDownloader + Sync + Send + 'static,
    JP: JsonPersister + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
>(
    rocks_db: Arc<Storage>,
    id: Pubkey,
    options: Options,
    json_downloader: Option<Arc<JD>>,
    json_persister: Option<Arc<JP>>,
    max_json_to_download: usize,
    storage_service_base_path: Option<String>,
    token_price_fetcher: Arc<TPF>,
    metrics: Arc<ApiMetricsConfig>,
    tree_gaps_checker: &Option<Arc<PPC>>,
) -> Result<Option<Asset>, StorageError> {
    let assets = asset::get_by_ids(
        rocks_db.clone(),
        vec![id],
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
        &None,
        token_price_fetcher,
        metrics,
        tree_gaps_checker,
    )
    .await?;

    let mut result = match &assets[0] {
        Some(asset) => asset_to_rpc(asset.clone(), &None),
        None => Ok(None),
    };

    if let Ok(Some(asset)) = &mut result {
        if let Some(base_url) = storage_service_base_path {
            let _ = populate_previews_slice(&base_url, &rocks_db, &mut [asset]).await;
        }
    }

    result
}
