use entities::api_req_params::Options;
use interface::json::{JsonDownloader, JsonPersister};
use rocks_db::Storage;
use sea_orm::DbErr;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

use crate::{dao::scopes, rpc::Asset};

use super::common::asset_to_rpc;

pub async fn get_asset_batch(
    rocks_db: Arc<Storage>,
    ids: Vec<Pubkey>,
    options: Options,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
) -> Result<Vec<Option<Asset>>, DbErr> {
    let assets = scopes::asset::get_by_ids(
        rocks_db,
        ids,
        options,
        json_downloader,
        json_persister,
        max_json_to_download,
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
