use crate::dao::{scopes, ConversionError, SearchAssetsQuery};
use crate::rpc::filter::AssetSorting;
use crate::rpc::response::AssetList;
use rocks_db::Storage;
use sea_orm::DatabaseConnection;
use sea_orm::DbErr;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

use super::common::asset_list_to_rpc;

#[allow(clippy::too_many_arguments)]
pub async fn search_assets(
    db: &DatabaseConnection,
    index_client: Arc<impl postgre_client::storage_traits::AssetPubkeyFilteredFetcher>,
    rocks_db: Arc<Storage>,
    filter: SearchAssetsQuery,
    sort_by: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
) -> Result<AssetList, DbErr> {
    let filter_result: &Result<postgre_client::model::SearchAssetsFilter, ConversionError> =
        &filter.try_into();
    if let Err(ConversionError::IncompatibleGroupingKey(_)) = filter_result {
        // If the error is IncompatibleGroupingKey, return an empty response
        return Ok(AssetList {
            total: 0,
            limit: limit as u32,
            ..AssetList::default()
        });
    }
    let filter = filter_result
        .as_ref()
        .map_err(|e| DbErr::Custom(e.to_string()))?;
    let keys = index_client
        .get_asset_pubkeys_filtered(filter, &sort_by.into(), limit, page, before, after)
        .await
        .map_err(DbErr::Custom)?;
    let asset_ids = keys
        .iter()
        .filter_map(|k| Pubkey::try_from(k.pubkey.clone()).ok())
        .collect::<Vec<Pubkey>>();
    //todo: there is an additional round trip to the db here, this should be optimized
    let assets = scopes::asset::get_by_ids(db, rocks_db, asset_ids).await?;
    let assets = assets.into_iter().flatten().collect::<Vec<_>>();
    let (items, errors) = asset_list_to_rpc(assets);
    let total = items.len() as u32;
    let before = keys.first().map(|k| k.sorting_id.clone());
    let after = keys.last().map(|k| k.sorting_id.clone());

    let resp = AssetList {
        total,
        limit: limit as u32,
        page: page.map(|x| x as u32),
        before,
        after,
        items,
        errors,
    };
    Ok(resp)
}
