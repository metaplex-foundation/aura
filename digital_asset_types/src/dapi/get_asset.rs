use std::sync::Arc;

use entities::api_req_params::Options;
use interface::json::{JsonDownloader, JsonPersister};
use solana_sdk::pubkey::Pubkey;

use interface::processing_possibility::ProcessingPossibilityChecker;
use rocks_db::Storage;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};
use usecase::error::DasApiError;

use crate::{dao::scopes, rpc::Asset};

use super::common::asset_to_rpc;

pub async fn get_asset(
    rocks_db: Arc<Storage>,
    id: Pubkey,
    options: Options,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) -> Result<Option<Asset>, DasApiError> {
    if !rocks_db.can_process_assets(&[id]).await {
        return Err(DasApiError::CannotServiceRequest);
    }
    let assets = scopes::asset::get_by_ids(
        rocks_db,
        vec![id],
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
        tasks,
    )
    .await
    .map_err(Into::<DasApiError>::into)?;

    match &assets[0] {
        Some(asset) => asset_to_rpc(asset.clone()).map_err(Into::<DasApiError>::into),
        None => Ok(None),
    }
}
