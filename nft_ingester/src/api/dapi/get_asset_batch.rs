use std::sync::Arc;

use entities::api_req_params::DisplayOptions;
use interface::{
    json::{JsonDownloader, JsonPersister},
    price_fetcher::TokenPriceFetcher,
    processing_possibility::ProcessingPossibilityChecker,
};
use metrics_utils::ApiMetricsConfig;
use rocks_db::{errors::StorageError, Storage};
use solana_sdk::pubkey::Pubkey;

use super::asset_preview::populate_previews_opt;
use crate::api::dapi::{asset, rpc_asset_convertors::asset_to_rpc, rpc_asset_models::Asset};

#[allow(clippy::too_many_arguments)]
pub async fn get_asset_batch<
    TPF: TokenPriceFetcher,
    JD: JsonDownloader + Sync + Send + 'static,
    JP: JsonPersister + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
>(
    rocks_db: Arc<Storage>,
    ids: Vec<Pubkey>,
    options: DisplayOptions,
    json_downloader: Option<Arc<JD>>,
    json_persister: Option<Arc<JP>>,
    max_json_to_download: usize,
    storage_service_base_path: Option<String>,
    token_price_fetcher: Arc<TPF>,
    metrics: Arc<ApiMetricsConfig>,
    tree_gaps_checker: &Option<Arc<PPC>>,
) -> Result<Vec<Option<Asset>>, StorageError> {
    let assets = asset::get_by_ids(
        rocks_db.clone(),
        ids,
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
