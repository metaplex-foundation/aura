use crate::error::IngesterError;
use entities::models::CompleteAssetDetails;
use entities::models::Updated;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::AssetDetailsStream;
use log::error;
use rocks_db::asset::{AssetCollection, AssetLeaf};
use rocks_db::cl_items::{ClItem, ClLeaf};
use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
use serde_json::json;
use std::sync::Arc;

pub async fn process_asset_details_stream(storage: Arc<Storage>, mut stream: AssetDetailsStream) {
    while let Some(result) = stream.next().await {
        match result {
            Ok(details) => {
                if let Some(e) = insert_gaped_data(storage.clone(), details).err() {
                    error!("Error processing gaped data: {}", e)
                }
            }
            Err(e) => {
                error!("Error processing stream item: {}", e);
            }
        }
    }
}

pub fn insert_gaped_data(
    rocks_storage: Arc<Storage>,
    data: CompleteAssetDetails,
) -> Result<(), IngesterError> {
    rocks_storage.asset_static_data.merge(
        data.pubkey,
        &AssetStaticDetails {
            pubkey: data.pubkey,
            specification_asset_class: data.specification_asset_class,
            royalty_target_type: data.royalty_target_type,
            created_at: data.slot_created as i64,
        },
    )?;

    rocks_storage.asset_dynamic_data.merge(
        data.pubkey,
        &AssetDynamicDetails {
            pubkey: data.pubkey,
            is_compressible: data.is_compressible,
            is_compressed: data.is_compressed,
            is_frozen: data.is_frozen,
            supply: data.supply,
            seq: data.seq,
            is_burnt: data.is_burnt,
            was_decompressed: data.was_decompressed,
            onchain_data: data.onchain_data.map(|chain_data| {
                Updated::new(
                    chain_data.slot_updated,
                    chain_data.seq,
                    json!(chain_data.value).to_string(),
                )
            }),
            creators: data.creators,
            royalty_amount: data.royalty_amount,
            url: data.url,
        },
    )?;

    rocks_storage.asset_authority_data.merge(
        data.pubkey,
        &AssetAuthority {
            pubkey: data.pubkey,
            authority: data.authority.value,
            slot_updated: data.authority.slot_updated,
        },
    )?;

    if let Some(collection) = data.collection {
        rocks_storage.asset_collection_data.merge(
            data.pubkey,
            &AssetCollection {
                pubkey: data.pubkey,
                collection: collection.value.collection,
                is_collection_verified: collection.value.is_collection_verified,
                collection_seq: collection.value.collection_seq,
                slot_updated: collection.slot_updated,
            },
        )?;
    }

    if let Some(leaf) = data.asset_leaf {
        rocks_storage.asset_leaf_data.merge(
            data.pubkey,
            &AssetLeaf {
                pubkey: data.pubkey,
                tree_id: leaf.value.tree_id,
                leaf: leaf.value.leaf.clone(),
                nonce: leaf.value.nonce,
                data_hash: leaf.value.data_hash,
                creator_hash: leaf.value.creator_hash,
                leaf_seq: leaf.value.leaf_seq,
                slot_updated: leaf.slot_updated,
            },
        )?
    }

    rocks_storage.asset_owner_data.merge(
        data.pubkey,
        &AssetOwner {
            pubkey: data.pubkey,
            owner: data.owner,
            delegate: data.delegate,
            owner_type: data.owner_type,
            owner_delegate_seq: data.owner_delegate_seq,
        },
    )?;

    if let Some(leaf) = data.cl_leaf {
        rocks_storage.cl_leafs.put(
            (leaf.cli_leaf_idx, leaf.cli_tree_key),
            ClLeaf {
                cli_leaf_idx: leaf.cli_leaf_idx,
                cli_tree_key: leaf.cli_tree_key,
                cli_node_idx: leaf.cli_node_idx,
            },
        )?
    }

    data.cl_items.iter().try_for_each(|item| {
        rocks_storage.cl_items.merge(
            (item.cli_node_idx, item.cli_tree_key),
            &ClItem {
                cli_node_idx: item.cli_node_idx,
                cli_tree_key: item.cli_tree_key,
                cli_leaf_idx: item.cli_leaf_idx,
                cli_seq: item.cli_seq,
                cli_level: item.cli_level,
                cli_hash: item.cli_hash.clone(),
                slot_updated: item.slot_updated,
            },
        )
    })?;

    Ok(())
}
