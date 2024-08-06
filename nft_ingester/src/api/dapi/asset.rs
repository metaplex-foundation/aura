use std::collections::HashMap;
use std::string::ToString;
use std::sync::Arc;

use entities::api_req_params::{AssetSortDirection, Options};
use entities::models::{AssetSignatureWithPagination, OffChainData};
use interface::asset_sigratures::AssetSignaturesGetter;
use interface::json::{JsonDownloader, JsonPersister};
use log::error;
use rocks_db::errors::StorageError;
use solana_sdk::pubkey::Pubkey;

use crate::api::dapi::rpc_asset_models::FullAsset;
use futures::{stream, StreamExt};
use interface::processing_possibility::ProcessingPossibilityChecker;
use rocks_db::asset::{AssetLeaf, AssetSelectedMaps};
use rocks_db::{AssetAuthority, Storage};
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub const COLLECTION_GROUP_KEY: &str = "collection";

fn convert_rocks_asset_model(
    asset_pubkey: &Pubkey,
    asset_selected_maps: &AssetSelectedMaps,
    offchain_data: OffChainData,
) -> Result<FullAsset, StorageError> {
    let static_data =
        asset_selected_maps
            .assets_static
            .get(asset_pubkey)
            .ok_or(StorageError::Common(
                "No relevant assets_static_data".to_string(),
            ))?;
    let dynamic_data =
        asset_selected_maps
            .assets_dynamic
            .get(asset_pubkey)
            .ok_or(StorageError::Common(
                "No relevant asset_dynamic_data".to_string(),
            ))?;
    let owner = asset_selected_maps
        .assets_owner
        .get(asset_pubkey)
        .ok_or(StorageError::Common(
            "No relevant assets_owners".to_string(),
        ))?;

    let leaf = asset_selected_maps
        .assets_leaf
        .get(asset_pubkey)
        .cloned()
        .unwrap_or(AssetLeaf::default()); // Asset may not have a leaf but we still can make the conversion

    Ok(FullAsset {
        asset_static: static_data.clone(),
        asset_owner: owner.clone(),
        asset_dynamic: dynamic_data.clone(),
        asset_leaf: leaf,
        offchain_data,
        asset_collections: asset_selected_maps
            .assets_collection
            .get(asset_pubkey)
            .cloned(),
        assets_authority: asset_selected_maps
            .assets_authority
            .get(asset_pubkey)
            .cloned()
            .unwrap_or(AssetAuthority::default()),
        edition_data: asset_selected_maps
            .assets_static
            .get(asset_pubkey)
            .and_then(|static_details| {
                static_details
                    .edition_address
                    .and_then(|e| asset_selected_maps.editions.get(&e).cloned())
            }),
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
    options: &Options,
) -> Option<FullAsset> {
    if !options.show_unverified_collections {
        if let Some(collection_data) = asset_selected_maps.assets_collection.get(id) {
            if !collection_data.is_collection_verified.value {
                return None;
            }
        } else {
            // don't have collection data == collection unverified
            return None;
        }
    }

    let offchain_data = asset_selected_maps
        .urls
        .get(&id.to_string())
        .and_then(|url| asset_selected_maps.offchain_data.get(url).cloned())
        .unwrap_or_default();

    convert_rocks_asset_model(id, asset_selected_maps, offchain_data).ok()
}

pub async fn get_by_ids(
    rocks_db: Arc<Storage>,
    asset_ids: Vec<Pubkey>,
    options: Options,
    json_downloader: Option<Arc<impl JsonDownloader + Sync + Send + 'static>>,
    json_persister: Option<Arc<impl JsonPersister + Sync + Send + 'static>>,
    max_json_to_download: usize,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) -> Result<Vec<Option<FullAsset>>, StorageError> {
    if asset_ids.is_empty() {
        return Ok(vec![]);
    }
    if !rocks_db.can_process_assets(asset_ids.as_slice()).await {
        return Err(StorageError::CannotServiceRequest);
    }
    // need to pass only unique asset_ids to select query
    // index need to save order of IDs in response
    let mut unique_asset_ids_map = HashMap::new();
    for (index, id) in asset_ids.iter().enumerate() {
        unique_asset_ids_map
            .entry(*id)
            .or_insert_with(Vec::new)
            .push(index);
    }

    let unique_asset_ids: Vec<_> = unique_asset_ids_map.keys().cloned().collect();
    let mut asset_selected_maps = rocks_db
        .get_asset_selected_maps_async(unique_asset_ids.clone())
        .await?;

    if let Some(json_downloader) = json_downloader {
        let mut urls_to_download = Vec::new();

        for (_, url) in asset_selected_maps.urls.iter() {
            if urls_to_download.len() >= max_json_to_download {
                break;
            }
            if !asset_selected_maps.offchain_data.contains_key(url) && !url.is_empty() {
                urls_to_download.push(url.clone());
            }
        }

        let num_of_tasks = urls_to_download.len();

        if num_of_tasks != 0 {
            let download_results = stream::iter(urls_to_download)
                .map(|url| {
                    let json_downloader = json_downloader.clone();

                    async move {
                        let response = json_downloader.download_file(url.clone()).await;
                        (url, response)
                    }
                })
                .buffered(num_of_tasks)
                .collect::<Vec<_>>()
                .await;

            for (json_url, res) in download_results.iter() {
                if let Ok(metadata) = res {
                    asset_selected_maps.offchain_data.insert(
                        json_url.clone(),
                        OffChainData {
                            url: json_url.clone(),
                            metadata: metadata.clone(),
                        },
                    );
                }
            }

            if let Some(json_persister) = json_persister {
                if !download_results.is_empty() {
                    let download_results = download_results.clone();
                    tasks.lock().await.spawn(async move {
                        if let Err(e) = json_persister.persist_response(download_results).await {
                            error!("Could not persist downloaded JSONs: {:?}", e);
                        }
                        Ok(())
                    });
                }
            }
        }
    }

    let mut results = vec![None; asset_ids.len()];
    for id in unique_asset_ids {
        let res = asset_selected_maps_into_full_asset(&id, &asset_selected_maps, &options);

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
        }
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
        }
        _ => {
            // If neither set of parameters is provided, return an error
            return Err(StorageError::Common(
                "Either 'id' or both 'tree' and 'leafIndex' must be provided".to_string(),
            ));
        }
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