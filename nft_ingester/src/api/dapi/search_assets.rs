use crate::api::dapi::asset;
use crate::api::dapi::converters::{ConversionError, SearchAssetsQuery};
use crate::api::dapi::response::{AssetList, NativeBalance};
use crate::api::dapi::rpc_asset_convertors::asset_list_to_rpc;
use crate::price_fetcher::SOLANA_CURRENCY;
use entities::api_req_params::{AssetSorting, SearchAssetsOptions};
use interface::account_balance::AccountBalanceGetter;
use interface::json::{JsonDownloader, JsonPersister};
use metrics_utils::ApiMetricsConfig;
use rocks_db::errors::StorageError;
use rocks_db::Storage;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;
use tracing::error;
use tracing::log::info;

use super::asset_preview::populate_previews;

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
    options: SearchAssetsOptions,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    account_balance_getter: Arc<impl AccountBalanceGetter>,
    storage_service_base_path: Option<String>,
    metrics: Arc<ApiMetricsConfig>,
) -> Result<AssetList, StorageError> {
    let begin_processing = Instant::now();
    let show_native_balance = options.show_native_balance;
    let (asset_list, native_balance) = tokio::join!(
        fetch_assets(
            index_client,
            rocks_db.clone(),
            filter.clone(),
            sort_by,
            limit,
            page,
            before,
            after,
            cursor,
            options,
            json_downloader,
            json_persister,
            max_json_to_download,
            tasks,
            metrics.clone(),
        ),
        fetch_native_balance(
            show_native_balance,
            filter.owner_address,
            account_balance_getter,
            rocks_db.clone(),
        )
    );
    info!(
        "total_request_processing: {}",
        begin_processing.elapsed().as_millis()
    );
    metrics.set_request_processing_latency(
        "total_request_processing",
        begin_processing.elapsed().as_millis() as f64,
    );

    let native_balance = native_balance.unwrap_or_else(|e| {
        error!("fetch_native_balance: {e}");
        None
    });
    let mut asset_list = asset_list?;
    asset_list.native_balance = native_balance;

    if let Some(base_url) = storage_service_base_path.as_ref() {
        let _ = populate_previews(base_url, &rocks_db, &mut asset_list.items).await;
    }

    Ok(asset_list)
}

#[allow(clippy::too_many_arguments)]
async fn fetch_assets(
    index_client: Arc<impl postgre_client::storage_traits::AssetPubkeyFilteredFetcher>,
    rocks_db: Arc<Storage>,
    filter: SearchAssetsQuery,
    sort_by: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
    cursor: Option<String>,
    options: SearchAssetsOptions,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    metrics: Arc<ApiMetricsConfig>,
) -> Result<AssetList, StorageError> {
    let begin_processing = Instant::now();
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
        .map_err(|e| StorageError::Common(e.to_string()))?; // TODO: change error

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
        .get_asset_pubkeys_filtered(
            filter,
            &sort_by.into(),
            limit,
            page,
            before,
            after,
            &(&options).into(),
        )
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?;
    let asset_ids = keys
        .iter()
        .filter_map(|k| Pubkey::try_from(k.pubkey.clone()).ok())
        .collect::<Vec<Pubkey>>();
    info!(
        "get_asset_pubkeys_filtered: {}",
        begin_processing.elapsed().as_millis()
    );
    metrics.set_request_processing_latency(
        "get_asset_pubkeys_filtered",
        begin_processing.elapsed().as_millis() as f64,
    );
    let begin_processing = Instant::now();

    //todo: there is an additional round trip to the db here, this should be optimized
    let assets = asset::get_by_ids(
        rocks_db,
        asset_ids,
        (&options).into(),
        json_downloader,
        json_persister,
        max_json_to_download,
        tasks,
    )
    .await?;
    info!("get_by_ids: {}", begin_processing.elapsed().as_millis());
    metrics.set_request_processing_latency(
        "get_by_ids",
        begin_processing.elapsed().as_millis() as f64,
    );

    let assets = assets.into_iter().flatten().collect::<Vec<_>>();
    let (items, errors) = asset_list_to_rpc(assets);
    let total = items.len() as u32;

    let (before, after, cursor, page_res) = if cursor_enabled {
        (None, None, keys.last().map(|k| k.sorting_id.clone()), None)
    } else if let Some(page) = page {
        (None, None, None, Some(page as u32))
    } else {
        (
            keys.first().map(|k| k.sorting_id.clone()),
            keys.last().map(|k| k.sorting_id.clone()),
            None,
            None,
        )
    };
    let mut grand_total = None;
    if options.show_grand_total {
        grand_total = Some(
            index_client
                .get_grand_total(filter, &(&options).into())
                .await
                .map_err(|e| StorageError::Common(e.to_string()))?,
        )
    }

    let resp = AssetList {
        total,
        grand_total,
        limit: limit as u32,
        page: page_res,
        before,
        after,
        items,
        errors,
        cursor,
        ..AssetList::default()
    };
    Ok(resp)
}

async fn fetch_native_balance(
    show_native_balance: bool,
    owner_address: Option<Vec<u8>>,
    account_balance_getter: Arc<impl AccountBalanceGetter>,
    rocks_db: Arc<Storage>,
) -> Result<Option<NativeBalance>, StorageError> {
    if !show_native_balance {
        return Ok(None);
    }
    let Some(owner_address) = owner_address else {
        return Ok(None);
    };
    let lamports =
        account_balance_getter
            .get_account_balance_lamports(&Pubkey::try_from(owner_address).map_err(|pk| {
                StorageError::Common(format!("Cannot convert public key: {:?}", pk))
            })?)
            .await
            .map_err(|e| StorageError::Common(format!("Account balance getter: {}", e)))?;
    let token_price = rocks_db
        .token_prices
        .get(SOLANA_CURRENCY.to_string())?
        .ok_or(StorageError::Common("Not token price".to_string()))?;

    Ok(Some(NativeBalance {
        lamports,
        price_per_sol: token_price.price,
        total_price: calculate_total_price_usd_by_lamports(lamports, token_price.price),
    }))
}

fn calculate_total_price_usd_by_lamports(lamports: u64, sol_price: f64) -> f64 {
    sol_price * (lamports as f64) / solana_sdk::native_token::LAMPORTS_PER_SOL as f64
}
