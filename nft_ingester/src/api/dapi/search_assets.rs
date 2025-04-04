use std::sync::Arc;

use entities::{
    api_req_params::{AssetSorting, GetByMethodsOptions},
    enums::TokenType,
};
use interface::{
    account_balance::AccountBalanceGetter,
    json::{JsonDownloader, JsonPersister},
    price_fetcher::TokenPriceFetcher,
    processing_possibility::ProcessingPossibilityChecker,
};
use metrics_utils::ApiMetricsConfig;
use rocks_db::{errors::StorageError, Storage};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

use super::asset_preview::populate_previews;
use crate::api::dapi::{
    asset,
    converters::{ConversionError, SearchAssetsQuery},
    response::{AssetList, NativeBalance},
    rpc_asset_convertors::asset_list_to_rpc,
};

#[allow(clippy::too_many_arguments)]
pub async fn search_assets<
    TPF: TokenPriceFetcher,
    JD: JsonDownloader + Sync + Send + 'static,
    JP: JsonPersister + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
>(
    index_client: Arc<impl postgre_client::storage_traits::AssetPubkeyFilteredFetcher>,
    rocks_db: Arc<Storage>,
    mut filter: SearchAssetsQuery,
    sort_by: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
    cursor: Option<String>,
    options: GetByMethodsOptions,
    json_downloader: Option<Arc<JD>>,
    json_persister: Option<Arc<JP>>,
    max_json_to_download: usize,
    account_balance_getter: Arc<impl AccountBalanceGetter>,
    storage_service_base_path: Option<String>,
    token_price_fetcher: Arc<TPF>,
    metrics: Arc<ApiMetricsConfig>,
    tree_gaps_checker: &Option<Arc<PPC>>,
    native_mint_pubkey: &str,
) -> Result<AssetList, StorageError> {
    if options.show_fungible {
        filter.token_type = Some(TokenType::All)
    }
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
            token_price_fetcher.clone(),
            metrics,
            tree_gaps_checker,
        ),
        fetch_native_balance(
            show_native_balance,
            filter.owner_address,
            account_balance_getter,
            token_price_fetcher.clone(),
            native_mint_pubkey
        )
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
async fn fetch_assets<
    TPF: TokenPriceFetcher,
    JD: JsonDownloader + Sync + Send + 'static,
    JP: JsonPersister + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
>(
    pg_index_client: Arc<impl postgre_client::storage_traits::AssetPubkeyFilteredFetcher>,
    rocks_db: Arc<Storage>,
    filter: SearchAssetsQuery,
    sort_by: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
    cursor: Option<String>,
    options: GetByMethodsOptions,
    json_downloader: Option<Arc<JD>>,
    json_persister: Option<Arc<JP>>,
    max_json_to_download: usize,
    token_price_fetcher: Arc<TPF>,
    metrics: Arc<ApiMetricsConfig>,
    tree_gaps_checker: &Option<Arc<PPC>>,
) -> Result<AssetList, StorageError> {
    let filter_result: &Result<postgre_client::model::SearchAssetsFilter, ConversionError> =
        &filter.try_into();
    if let Err(ConversionError::IncompatibleGroupingKey(_)) = filter_result {
        // If the error is IncompatibleGroupingKey, return an empty response
        return Ok(AssetList { total: 0, limit: limit as u32, ..AssetList::default() });
    }
    let filter = filter_result.as_ref().map_err(|e| StorageError::Common(e.to_string()))?; // TODO: change error

    let cursor_enabled = before.is_none() && after.is_none() && page.is_none();

    // if cursor is passed use it as 'after' parameter
    let after = {
        if cursor_enabled {
            cursor
        } else {
            after
        }
    };

    let keys = pg_index_client
        .get_asset_pubkeys_filtered(filter, &sort_by.into(), limit, page, before, after, &options)
        .await
        .map_err(|e| {
            if e.to_string().contains("statement timeout") {
                StorageError::QueryTimedOut
            } else {
                StorageError::Common(e.to_string())
            }
        })?;

    let asset_ids = keys
        .iter()
        .filter_map(|k| Pubkey::try_from(k.pubkey.clone()).ok())
        .collect::<Vec<Pubkey>>();

    let owner_address = filter
        .token_type
        .clone()
        .zip(filter.owner_address.clone())
        .and_then(|(token_type, owner_address)| {
            if token_type == TokenType::All || token_type == TokenType::Fungible {
                return Some(Pubkey::try_from(owner_address).ok());
            }
            None
        })
        .flatten();

    //todo: there is an additional round trip to the db here, this should be optimized
    let assets = asset::get_by_ids(
        rocks_db,
        asset_ids,
        options.clone().into(),
        json_downloader,
        json_persister,
        max_json_to_download,
        &owner_address,
        token_price_fetcher,
        metrics,
        tree_gaps_checker,
    )
    .await?;

    let assets = assets.into_iter().flatten().collect::<Vec<_>>();
    let (items, errors) = asset_list_to_rpc(assets, &owner_address);
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
        grand_total =
            Some(pg_index_client.get_grand_total(filter, &options).await.map_err(|e| {
                if e.to_string().contains("statement timeout") {
                    StorageError::QueryTimedOut
                } else {
                    StorageError::Common(e.to_string())
                }
            })?)
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

async fn fetch_native_balance<TPF: TokenPriceFetcher>(
    show_native_balance: bool,
    owner_address: Option<Vec<u8>>,
    account_balance_getter: Arc<impl AccountBalanceGetter>,
    token_price_fetcher: Arc<TPF>,
    native_mint_pubkey: &str,
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
    let token_price = *token_price_fetcher
        .fetch_token_prices(&[native_mint_pubkey.to_string()])
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
        .get(&spl_token::native_mint::id().to_string())
        .ok_or(StorageError::Common("Not token price".to_string()))?;

    Ok(Some(NativeBalance {
        lamports,
        price_per_sol: token_price,
        total_price: calculate_total_price_usd_by_lamports(lamports, token_price),
    }))
}

fn calculate_total_price_usd_by_lamports(lamports: u64, sol_price: f64) -> f64 {
    sol_price * (lamports as f64) / solana_sdk::native_token::LAMPORTS_PER_SOL as f64
}
