use std::sync::Arc;

use entities::api_req_params::Options;
use interface::json::{JsonDownloader, JsonPersister};
use rocks_db::{errors::StorageError, Storage};
use solana_sdk::pubkey::Pubkey;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};

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
) -> Result<Option<Asset>, StorageError> {
    let assets = scopes::asset::get_by_ids(
        rocks_db,
        vec![id],
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
        tasks,
    )
    .await?;

    match &assets[0] {
        Some(asset) => asset_to_rpc(asset.clone()),
        None => Ok(None),
    }
}
