use std::sync::Arc;

use entities::api_req_params::AssetSorting;
use sea_orm::DatabaseConnection;
use sea_orm::DbErr;

use rocks_db::Storage;

use crate::dao::scopes;
use crate::rpc::response::AssetList;

use super::common::{build_asset_response, create_pagination, create_sorting};

#[allow(clippy::too_many_arguments)]
pub async fn get_assets_by_group(
    db: &DatabaseConnection,
    rocks_db: Arc<Storage>,
    group_value: Vec<u8>,
    group_key: String,
    sorting: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
) -> Result<AssetList, DbErr> {
    let pagination = create_pagination(before, after, page)?;
    let (sort_direction, sort_column) = create_sorting(sorting);
    let assets = scopes::asset::get_by_grouping(
        db,
        rocks_db,
        group_value,
        group_key,
        sort_column,
        sort_direction,
        &pagination,
        limit,
    )
    .await?;
    Ok(build_asset_response(assets, limit, &pagination))
}
