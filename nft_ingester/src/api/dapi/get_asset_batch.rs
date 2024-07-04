use entities::api_req_params::Options;
use interface::json::{JsonDownloader, JsonPersister};
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

pub async fn get_asset_batch(
    rocks_db: Arc<Storage>,
    ids: Vec<Pubkey>,
    options: Options,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) -> Result<Vec<Option<Asset>>, StorageError> {
    let assets = asset::get_by_ids(
        rocks_db,
        ids,
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
        tasks,
    )
    .await?;

    assets
        .into_iter()
        .map(|asset| match asset {
            Some(asset) => Ok(Some(asset_to_rpc(asset)?.unwrap())),
            None => Ok(None),
        })
        .collect()
}
