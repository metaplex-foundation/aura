use entities::api_req_params::Options;
use interface::json::{JsonDownloader, JsonPersister};
use interface::processing_possibility::ProcessingPossibilityChecker;
use rocks_db::Storage;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};
use usecase::error::DasApiError;

use crate::{dao::scopes, rpc::Asset};

use super::common::asset_to_rpc;

pub async fn get_asset_batch(
    rocks_db: Arc<Storage>,
    ids: Vec<Pubkey>,
    options: Options,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) -> Result<Vec<Option<Asset>>, DasApiError> {
    if !rocks_db.can_process_assets(ids.as_slice()).await {
        return Err(DasApiError::CannotServiceRequest);
    }
    let assets = scopes::asset::get_by_ids(
        rocks_db,
        ids,
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
        tasks,
    )
    .await
    .map_err(Into::<DasApiError>::into)?;

    assets
        .into_iter()
        .map(|asset| match asset {
            Some(asset) => Ok(Some(
                asset_to_rpc(asset)
                    .map_err(Into::<DasApiError>::into)?
                    .unwrap(),
            )),
            None => Ok(None),
        })
        .collect()
}
