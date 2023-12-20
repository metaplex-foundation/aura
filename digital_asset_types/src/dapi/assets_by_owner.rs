use std::sync::Arc;

use sea_orm::DatabaseConnection;
use sea_orm::DbErr;
use solana_sdk::pubkey::Pubkey;

use rocks_db::Storage;

use crate::dao::scopes;
use crate::rpc::filter::AssetSorting;
use crate::rpc::response::AssetList;

use super::common::{build_asset_response, create_pagination, create_sorting};

#[allow(clippy::too_many_arguments)]
pub async fn get_assets_by_owner(
    db: &DatabaseConnection,
    rocks_db: Arc<Storage>,
    owner_address: Pubkey,
    sort_by: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
) -> Result<AssetList, DbErr> {
    let pagination = create_pagination(before, after, page)?;
    let (sort_direction, sort_column) = create_sorting(sort_by);
    let assets = scopes::asset::get_assets_by_owner(
        db,
        rocks_db,
        owner_address,
        sort_column,
        sort_direction,
        &pagination,
        limit,
    )
    .await?;
    Ok(build_asset_response(assets, limit, &pagination))
}
