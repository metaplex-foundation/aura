use std::{collections::HashMap, string::ToString, sync::Arc, time::Duration};

use entities::{
    api_req_params::{AssetSortDirection, Options},
    enums::SpecificationAssetClass,
    models::{AssetSignatureWithPagination, MetadataDownloadTask},
};
use futures::{stream, StreamExt};
use interface::{
    asset_sigratures::AssetSignaturesGetter,
    json::{JsonDownloadResult, JsonDownloader, JsonPersister, MetadataDownloadResult},
    price_fetcher::TokenPriceFetcher,
    processing_possibility::ProcessingPossibilityChecker,
};
use itertools::Itertools;
use metrics_utils::ApiMetricsConfig;
use rocks_db::{
    columns::{
        asset::{AssetLeaf, AssetSelectedMaps},
        offchain_data::{OffChainData, StorageMutability},
    },
    errors::StorageError,
    Storage,
};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

use crate::api::dapi::rpc_asset_models::FullAsset;

pub const COLLECTION_GROUP_KEY: &str = "collection";
pub const METADATA_CACHE_TTL: i64 = 86400; // 1 day
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(3);

fn convert_rocks_asset_model(
    asset_pubkey: &Pubkey,
    asset_selected_maps: &AssetSelectedMaps,
    token_prices: &HashMap<String, f64>,
    token_symbols: &HashMap<String, String>,
    offchain_data: OffChainData,
) -> Result<FullAsset, StorageError> {
    let data = asset_selected_maps
        .asset_complete_details
        .get(asset_pubkey)
        .ok_or(StorageError::Common("No relevant asset_complete_details".to_string()))?;
    let static_data = data
        .static_details
        .as_ref()
        .ok_or(StorageError::Common("No relevant assets_static_data".to_string()))?;
    let dynamic_data = data
        .dynamic_details
        .as_ref()
        .ok_or(StorageError::Common("No relevant asset_dynamic_data".to_string()))?;
    let owner =
        data.owner.as_ref().ok_or(StorageError::Common("No relevant assets_owners".to_string()))?;

    let leaf =
        asset_selected_maps.assets_leaf.get(asset_pubkey).cloned().unwrap_or(AssetLeaf::default()); // Asset may not have a leaf but we still can make the conversion

    let collection_data = data
        .collection
        .as_ref()
        .and_then(|collection| {
            asset_selected_maps.asset_complete_details.get(&collection.collection.value)
        })
        .cloned();
    let collection_dynamic_data = collection_data.as_ref().and_then(|c| c.dynamic_details.clone());

    let inscription = asset_selected_maps.inscriptions.get(asset_pubkey).cloned();
    Ok(FullAsset {
        asset_static: static_data.clone(),
        asset_owner: owner.clone(),
        asset_dynamic: dynamic_data.clone(),
        asset_leaf: leaf,
        offchain_data,
        asset_collections: data.collection.clone(),
        assets_authority: data.authority.clone(),
        edition_data: static_data
            .edition_address
            .and_then(|e| asset_selected_maps.editions.get(&e).cloned()),
        mpl_core_collections: data
            .collection
            .as_ref()
            .and_then(|collection| {
                asset_selected_maps.mpl_core_collections.get(&collection.collection.value)
            })
            .cloned(),
        collection_offchain_data: collection_dynamic_data
            .as_ref()
            .and_then(|dynamic_data| asset_selected_maps.offchain_data.get(&dynamic_data.url.value))
            .cloned(),
        collection_dynamic_data,
        inscription_data: inscription
            .as_ref()
            .and_then(|inscription| {
                asset_selected_maps.inscriptions_data.get(&inscription.inscription_data_account)
            })
            .cloned(),
        token_account: asset_selected_maps.token_accounts.get(asset_pubkey).cloned(),
        inscription,
        spl_mint: asset_selected_maps.spl_mints.get(asset_pubkey).cloned(),

        token_symbol: token_symbols.get(&asset_pubkey.to_string()).cloned(),
        token_price: token_prices.get(&asset_pubkey.to_string()).cloned(),
    })
}

// Use macros to reduce code duplications
#[macro_export]
macro_rules! fetch_asset_data {
    ($db:expr, $field:ident, $asset_ids:expr) => {{
        $db.$field
            .batch_get($asset_ids.clone())
            .await?
            .into_iter()
            .filter_map(|asset| asset.map(|a| (a.pubkey, a)))
            .collect::<HashMap<_, _>>()
    }};
}

fn asset_selected_maps_into_full_asset(
    id: &Pubkey,
    asset_selected_maps: &AssetSelectedMaps,
    token_prices: &HashMap<String, f64>,
    token_symbols: &HashMap<String, String>,
    options: &Options,
) -> Option<FullAsset> {
    if !options.show_unverified_collections {
        if let Some(asset_complete_details) = asset_selected_maps.asset_complete_details.get(id) {
            if let Some(asset_static_details) = &asset_complete_details.static_details {
                // collection itself cannot have a collection
                // TODO!: should we also include in this check FungibleToken?
                if asset_static_details.specification_asset_class
                    != SpecificationAssetClass::MplCoreCollection
                {
                    if let Some(collection_details) = &asset_complete_details.collection {
                        if !collection_details.is_collection_verified.value {
                            return None;
                        }
                    }
                }
            }
        }
    }

    let offchain_data = asset_selected_maps
        .urls
        .get(id)
        .and_then(|url| asset_selected_maps.offchain_data.get(url).cloned())
        .unwrap_or_default();

    convert_rocks_asset_model(id, asset_selected_maps, token_prices, token_symbols, offchain_data)
        .ok()
}

#[allow(clippy::too_many_arguments)]
pub async fn get_by_ids<
    TPF: TokenPriceFetcher,
    JD: JsonDownloader + Sync + Send + 'static,
    JP: JsonPersister + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
>(
    rocks_db: Arc<Storage>,
    asset_ids: Vec<Pubkey>,
    options: Options,
    json_downloader: Option<Arc<JD>>,
    json_persister: Option<Arc<JP>>,
    max_json_to_download: usize,
    // We need owner_address if we want to query fungible token accounts
    owner_address: &Option<Pubkey>,
    token_price_fetcher: Arc<TPF>,
    metrics: Arc<ApiMetricsConfig>,
    tree_gaps_checker: &Option<Arc<PPC>>,
) -> Result<Vec<Option<FullAsset>>, StorageError> {
    if asset_ids.is_empty() {
        return Ok(vec![]);
    }
    // if at least one asset is from inconsistent tree request will not be processed
    if let Some(tree_gaps_checker) = tree_gaps_checker {
        if !tree_gaps_checker.can_process_assets(asset_ids.as_slice()).await {
            return Err(StorageError::CannotServiceRequest);
        }
    }
    // need to pass only unique asset_ids to select query
    // index need to save order of IDs in response
    let mut unique_asset_ids_map = HashMap::new();
    for (index, id) in asset_ids.iter().enumerate() {
        unique_asset_ids_map.entry(*id).or_insert_with(Vec::new).push(index);
    }

    let unique_asset_ids: Vec<Pubkey> = unique_asset_ids_map.keys().cloned().collect();
    let asset_ids_string = asset_ids.clone().into_iter().map(|id| id.to_string()).collect_vec();

    let (token_prices, token_symbols) = if options.show_fungible {
        let token_prices_fut = token_price_fetcher.fetch_token_prices(asset_ids_string.as_slice());
        let token_symbols_fut =
            token_price_fetcher.fetch_token_symbols(asset_ids_string.as_slice());
        tokio::join!(token_prices_fut, token_symbols_fut)
    } else {
        (Ok(HashMap::new()), Ok(HashMap::new()))
    };

    let token_prices = token_prices.unwrap_or_else(|e| {
        error!("Fetch token prices: {}", e);
        metrics.inc_token_info_fetch_errors("prices");
        HashMap::new()
    });
    let token_symbols = token_symbols.unwrap_or_else(|e| {
        error!("Fetch token symbols: {}", e);
        metrics.inc_token_info_fetch_errors("symbols");
        HashMap::new()
    });

    // request prices and symbols only for fungibles when the option is set. This will prolong the request at least an order of magnitude
    let mut asset_selected_maps = rocks_db
        .get_asset_selected_maps_async(unique_asset_ids.clone(), owner_address, &options)
        .await?;

    if let Some(json_downloader) = json_downloader {
        let mut urls_to_download = Vec::new();

        for (_, url) in asset_selected_maps.urls.iter() {
            if url.is_empty() {
                continue;
            }
            let offchain_data = asset_selected_maps.offchain_data.get(url);
            let mut download_needed = false;
            match offchain_data {
                Some(offchain_data) => {
                    let curr_time = chrono::Utc::now().timestamp();
                    if offchain_data.storage_mutability.is_mutable()
                        && curr_time > offchain_data.last_read_at + METADATA_CACHE_TTL
                        && !json_downloader.skip_refresh()
                    {
                        download_needed = true;
                    }

                    match &offchain_data.metadata {
                        Some(metadata) => {
                            if metadata.is_empty() {
                                download_needed = true;
                            }
                        },
                        None => {
                            download_needed = true;
                        },
                    }
                },
                None => {
                    download_needed = true;
                },
            }

            if download_needed {
                urls_to_download.push(url.clone());
            }

            if urls_to_download.len() >= max_json_to_download {
                break;
            }
        }

        let num_of_tasks = urls_to_download.len();

        if num_of_tasks > 0 {
            let download_results = stream::iter(urls_to_download)
                .map(|url| {
                    let json_downloader = json_downloader.clone();

                    // TODO: get etag and last_modified_at from the database
                    let metadata_download_task = MetadataDownloadTask {
                        metadata_url: url.clone(),
                        status: entities::enums::TaskStatus::Success,
                        etag: None,
                        last_modified_at: None,
                    };

                    async move {
                        let response = json_downloader
                            .download_file(&metadata_download_task, CLIENT_TIMEOUT)
                            .await;
                        (url, response)
                    }
                })
                .buffered(num_of_tasks)
                .collect::<Vec<_>>()
                .await;

            for (json_url, download_result) in download_results.iter() {
                let last_read_at = chrono::Utc::now().timestamp();
                match download_result {
                    Ok(MetadataDownloadResult {
                        result: JsonDownloadResult::JsonContent(metadata),
                        ..
                    }) => {
                        asset_selected_maps.offchain_data.insert(
                            json_url.clone(),
                            OffChainData {
                                url: Some(json_url.clone()),
                                metadata: Some(metadata.clone()),
                                storage_mutability: StorageMutability::from(json_url.as_str()),
                                last_read_at,
                            },
                        );
                    },
                    Ok(MetadataDownloadResult {
                        result: JsonDownloadResult::MediaUrlAndMimeType { url, mime_type },
                        ..
                    }) => {
                        asset_selected_maps.offchain_data.insert(
                            json_url.clone(),
                            OffChainData {
                                url: Some(json_url.clone()),
                                metadata: Some(
                                    format!("{{\"image\":\"{}\",\"type\":\"{}\"}}", url, mime_type)
                                        .to_string(),
                                ),
                                storage_mutability: StorageMutability::from(json_url.as_str()),
                                last_read_at,
                            },
                        );
                    },
                    Ok(MetadataDownloadResult {
                        result: JsonDownloadResult::NotModified, ..
                    }) => {},
                    Err(_) => {},
                }
            }

            if let Some(json_persister) = json_persister {
                if !download_results.is_empty() {
                    usecase::executor::spawn(async move {
                        if let Err(e) = json_persister.persist_response(download_results).await {
                            error!("Could not persist downloaded JSONs: {:?}", e);
                        }
                    });
                }
            }
        }
    }

    let mut results = vec![None; asset_ids.len()];
    for id in unique_asset_ids {
        let res = asset_selected_maps_into_full_asset(
            &id,
            &asset_selected_maps,
            &token_prices,
            &token_symbols,
            &options,
        );

        if let Some(indexes) = unique_asset_ids_map.get(&id) {
            for &index in indexes {
                results[index] = res.clone();
            }
        }
    }

    Ok(results)
}

#[allow(clippy::too_many_arguments)]
pub async fn get_asset_signatures(
    storage: Arc<Storage>,
    asset_id: Option<Pubkey>,
    tree_id: Option<Pubkey>,
    leaf_idx: Option<u64>,
    page: Option<u64>,
    before: &Option<String>,
    after: &Option<String>,
    limit: u64,
    sort_direction: Option<AssetSortDirection>,
) -> Result<AssetSignatureWithPagination, StorageError> {
    let before_sequence = before.as_ref().and_then(|b| b.parse::<u64>().ok());
    let after_sequence = after.as_ref().and_then(|a| a.parse::<u64>().ok());
    if let (Some(before_sequence), Some(after_sequence)) = (before_sequence, after_sequence) {
        let invalid_range = match sort_direction {
            Some(AssetSortDirection::Asc) => before_sequence <= after_sequence,
            _ => before_sequence >= after_sequence,
        };
        if invalid_range {
            return Ok(AssetSignatureWithPagination::default());
        }
    }
    let sort_direction = sort_direction.unwrap_or(AssetSortDirection::Desc);
    let (tree_id, leaf_idx) = match (tree_id, leaf_idx, asset_id) {
        (Some(tree_id), Some(leaf_idx), None) => {
            // Directly use tree_id and leaf_idx if both are provided
            (tree_id, leaf_idx)
        },
        (None, None, Some(asset_id)) => {
            // if only asset_id is provided, fetch the latest tree and leaf_idx (asset.nonce) for the asset
            // and use them to fetch transactions
            let asset_leaf = storage
                .asset_leaf_data
                .get(asset_id)?
                .ok_or_else(|| StorageError::Common("Leaf ID does not exist".to_string()))?; // Not found error
            (
                asset_leaf.tree_id,
                asset_leaf.nonce.ok_or_else(|| {
                    StorageError::Common("Leaf nonce does not exist".to_string())
                    // Not found error
                })?,
            )
        },
        _ => {
            // If neither set of parameters is provided, return an error
            return Err(StorageError::Common(
                "Either 'id' or both 'tree' and 'leafIndex' must be provided".to_string(),
            ));
        },
    };

    Ok(storage.get_asset_signatures(
        tree_id,
        leaf_idx,
        before_sequence,
        after_sequence,
        page,
        sort_direction,
        limit,
    ))
}
