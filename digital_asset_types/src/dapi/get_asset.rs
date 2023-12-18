use rocks_db::Storage;
use sea_orm::{DatabaseConnection, DbErr};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

use crate::{dao::scopes, rpc::Asset};

use super::common::asset_to_rpc;

pub async fn get_asset(
    db: &DatabaseConnection,
    rocks_db: Arc<Storage>,
    id: Pubkey,
) -> Result<Option<Asset>, DbErr> {
    let assets = scopes::asset::get_by_ids(db, rocks_db, vec![id]).await?;

    match &assets[0] {
        Some(asset) => asset_to_rpc(asset.clone()),
        None => Ok(None),
    }
}
