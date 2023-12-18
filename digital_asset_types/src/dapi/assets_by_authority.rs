use crate::dao::scopes;
use crate::rpc::filter::AssetSorting;
use crate::rpc::response::AssetList;
use rocks_db::Storage;
use sea_orm::DatabaseConnection;
use sea_orm::DbErr;
use std::sync::Arc;

use super::common::{build_asset_response, create_pagination, create_sorting};

pub async fn get_assets_by_authority(
    db: &DatabaseConnection,
    rocks_db: Arc<Storage>,
    authority: Vec<u8>,
    sorting: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
) -> Result<AssetList, DbErr> {
    let pagination = create_pagination(before, after, page)?;
    let (sort_direction, sort_column) = create_sorting(sorting);
    let assets = scopes::asset::get_by_authority(
        db,
        rocks_db,
        authority,
        sort_column,
        sort_direction,
        &pagination,
        limit,
    )
    .await?;

    Ok(build_asset_response(assets, limit, &pagination))
}
