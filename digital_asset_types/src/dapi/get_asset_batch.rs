use rocks_db::Storage;
use sea_orm::{DatabaseConnection, DbErr};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

use crate::{dao::scopes, rpc::Asset};

use super::common::asset_to_rpc;

pub async fn get_asset_batch(
    db: &DatabaseConnection,
    rocks_db: Arc<Storage>,
    ids: Vec<Pubkey>,
) -> Result<Vec<Option<Asset>>, DbErr> {
    let assets = scopes::asset::get_by_ids(db, rocks_db, ids).await?;

    assets
        .into_iter()
        .map(|asset| match asset {
            Some(asset) => Ok(Some(asset_to_rpc(asset)?.unwrap())),
            None => Ok(None),
        })
        .collect()
}
