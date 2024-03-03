use crate::dao::{scopes, ConversionError, SearchAssetsQuery};
use crate::rpc::response::AssetList;
use entities::api_req_params::AssetSorting;
use rocks_db::Storage;
use sea_orm::DbErr;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

use super::common::asset_list_to_rpc;

#[allow(clippy::too_many_arguments)]
pub async fn search_assets(
    index_client: Arc<impl postgre_client::storage_traits::AssetPubkeyFilteredFetcher>,
    rocks_db: Arc<Storage>,
    filter: SearchAssetsQuery,
    sort_by: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
    cursor: Option<String>,
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

    let cursor_enabled = before.is_none() && after.is_none() && page.is_none();

    // if cursor is passed use it as 'after' parameter
    let after = {
        if cursor_enabled {
            cursor
        } else {
            after
        }
    };

    let keys = index_client
        .get_asset_pubkeys_filtered(filter, &sort_by.into(), limit, page, before, after)
        .await
        .map_err(DbErr::Custom)?;
    let asset_ids = keys
        .iter()
        .filter_map(|k| Pubkey::try_from(k.pubkey.clone()).ok())
        .collect::<Vec<Pubkey>>();
    //todo: there is an additional round trip to the db here, this should be optimized
    let assets = scopes::asset::get_by_ids(rocks_db, asset_ids).await?;
    let assets = assets.into_iter().flatten().collect::<Vec<_>>();
    let (items, errors) = asset_list_to_rpc(assets);
    let total = items.len() as u32;

    let before;
    let after;
    let cursor;
    let page_res;

    if cursor_enabled {
        before = None;
        after = None;
        cursor = keys.last().map(|k| k.sorting_id.clone());
        page_res = None;
    } else if page.is_some() {
        before = None;
        after = None;
        cursor = None;
        page_res = page.map(|x| x as u32);
    } else {
        before = keys.first().map(|k| k.sorting_id.clone());
        after = keys.last().map(|k| k.sorting_id.clone());
        cursor = None;
        page_res = None;
    }

    let resp = AssetList {
        total,
        limit: limit as u32,
        page: page_res,
        before,
        after,
        items,
        errors,
        cursor,
    };
    Ok(resp)
}
