use std::sync::Arc;

use async_trait::async_trait;
use entities::models::{CompleteAssetDetails, Updated};
use interface::asset_streaming_and_discovery::{
    AssetDetailsStream, AssetDetailsStreamer, AsyncError,
};
use rocksdb::DB;
use solana_sdk::pubkey::Pubkey;
use tokio_stream::wrappers::ReceiverStream;

use crate::cl_items::{ClItem, ClLeaf};
use crate::{
    asset::{AssetCollection, AssetLeaf, SlotAssetIdx},
    column::TypedColumn,
    errors::StorageError,
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage,
};

#[async_trait]
impl AssetDetailsStreamer for Storage {
    async fn get_asset_details_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, AsyncError> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let backend = self.slot_asset_idx.backend.clone();

        self.join_set.lock().await.spawn(tokio::spawn(async move {
            let _ = process_asset_details_range(backend, start_slot, end_slot, tx.clone()).await;
        }));

        Ok(Box::pin(ReceiverStream::new(rx)) as AssetDetailsStream)
    }
}

async fn process_asset_details_range(
    backend: Arc<DB>,
    start_slot: u64,
    end_slot: u64,
    tx: tokio::sync::mpsc::Sender<Result<CompleteAssetDetails, AsyncError>>,
) -> Result<(), AsyncError> {
    let slot_asset_idx = Storage::column::<SlotAssetIdx>(backend.clone());
    let iterator = slot_asset_idx.iter((start_slot, solana_sdk::pubkey::Pubkey::default()));

    for pair in iterator {
        let (idx_key, _) = pair.map_err(|e| Box::new(e) as AsyncError)?;
        let (slot, pubkey) =
            SlotAssetIdx::decode_key(idx_key.to_vec()).map_err(|e| Box::new(e) as AsyncError)?;
        if slot > end_slot {
            break;
        }

        let details = get_complete_asset_details(backend.clone(), pubkey).await;
        match details {
            Err(e) => {
                if tx.send(Err(Box::new(e) as AsyncError)).await.is_err() {
                    break; // Receiver is dropped
                }
            }
            Ok(details) => {
                if tx.send(Ok(details)).await.is_err() {
                    break; // Receiver is dropped
                }
            }
        }
    }

    Ok(())
}

async fn get_complete_asset_details(
    backend: Arc<DB>,
    pubkey: Pubkey,
) -> crate::Result<CompleteAssetDetails> {
    let static_data = Storage::column::<AssetStaticDetails>(backend.clone()).get(pubkey)?;
    let static_data = match static_data {
        None => {
            return Err(crate::errors::StorageError::Common(
                "Asset static data not found".to_string(),
            ));
        }
        Some(static_data) => static_data,
    };

    let dynamic_data = Storage::column::<AssetDynamicDetails>(backend.clone()).get(pubkey)?;
    let dynamic_data = match dynamic_data {
        None => {
            return Err(crate::errors::StorageError::Common(
                "Asset dynamic data not found".to_string(),
            ));
        }
        Some(dynamic_data) => dynamic_data,
    };
    let authority = Storage::column::<AssetAuthority>(backend.clone()).get(pubkey)?;
    let authority = match authority {
        None => {
            return Err(crate::errors::StorageError::Common(
                "Asset authority not found".to_string(),
            ));
        }
        Some(authority) => authority,
    };
    let owner = Storage::column::<AssetOwner>(backend.clone()).get(pubkey)?;
    let owner = match owner {
        None => {
            return Err(crate::errors::StorageError::Common(
                "Asset owner not found".to_string(),
            ));
        }
        Some(owner) => owner,
    };

    let asset_leaf = Storage::column::<AssetLeaf>(backend.clone()).get(pubkey)?;
    let collection = Storage::column::<AssetCollection>(backend.clone()).get(pubkey)?;

    let onchain_data = match dynamic_data.onchain_data {
        None => None,
        Some(onchain_data) => {
            let v = serde_json::from_str(&onchain_data.value)
                .map_err(|e| StorageError::Common(e.to_string()))?;
            Some(Updated::new(onchain_data.slot_updated, onchain_data.seq, v))
        }
    };

    let cl_leaf = match asset_leaf {
        None => None,
        Some(ref leaf) => Storage::column::<ClLeaf>(backend.clone())
            .get((leaf.nonce.unwrap_or_default(), leaf.tree_id))?,
    };

    let cl_items = match cl_leaf {
        None => vec![],
        Some(ref leaf) => {
            Storage::column::<ClItem>(backend.clone())
                .batch_get(
                    get_required_nodes_for_proof(leaf.cli_node_idx as i64)
                        .into_iter()
                        .map(move |node| (node as u64, leaf.cli_tree_key))
                        .collect::<Vec<_>>(),
                )
                .await?
        }
    }
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    Ok(CompleteAssetDetails {
        pubkey: static_data.pubkey,
        specification_asset_class: static_data.specification_asset_class,
        royalty_target_type: static_data.royalty_target_type,
        slot_created: static_data.created_at as u64,
        edition_address: static_data.edition_address,
        is_compressible: dynamic_data.is_compressible,
        is_compressed: dynamic_data.is_compressed,
        is_frozen: dynamic_data.is_frozen,
        supply: dynamic_data.supply,
        seq: dynamic_data.seq,
        is_burnt: dynamic_data.is_burnt,
        was_decompressed: dynamic_data.was_decompressed,
        onchain_data,
        creators: dynamic_data.creators,
        royalty_amount: dynamic_data.royalty_amount,
        url: dynamic_data.url,
        chain_mutability: dynamic_data.chain_mutability,
        lamports: dynamic_data.lamports,
        executable: dynamic_data.executable,
        metadata_owner: dynamic_data.metadata_owner,
        authority: Updated::new(
            authority.slot_updated,
            None, //todo: where do we get seq?
            authority.authority,
        ),
        owner: owner.owner,
        delegate: owner.delegate,
        owner_type: owner.owner_type,
        owner_delegate_seq: owner.owner_delegate_seq,
        collection: collection.map(|collection| {
            Updated::new(
                collection.slot_updated,
                None, //todo: where do we get seq?
                entities::models::AssetCollection {
                    collection: collection.collection,
                    is_collection_verified: collection.is_collection_verified,
                    collection_seq: collection.collection_seq,
                },
            )
        }),
        cl_leaf: cl_leaf.map(|leaf| entities::models::ClLeaf {
            cli_leaf_idx: leaf.cli_leaf_idx,
            cli_tree_key: leaf.cli_tree_key,
            cli_node_idx: leaf.cli_node_idx,
        }),
        asset_leaf: asset_leaf.map(|leaf| {
            Updated::new(
                leaf.slot_updated,
                None,
                entities::models::AssetLeaf {
                    leaf: leaf.leaf,
                    tree_id: leaf.tree_id,
                    nonce: leaf.nonce,
                    data_hash: leaf.data_hash,
                    creator_hash: leaf.creator_hash,
                    leaf_seq: leaf.leaf_seq,
                },
            )
        }),
        cl_items: cl_items
            .into_iter()
            .map(|item| entities::models::ClItem {
                cli_node_idx: item.cli_node_idx,
                cli_tree_key: item.cli_tree_key,
                cli_leaf_idx: item.cli_leaf_idx,
                cli_seq: item.cli_seq,
                cli_level: item.cli_level,
                cli_hash: item.cli_hash,
                slot_updated: item.slot_updated,
            })
            .collect(),
    })
}

pub fn get_required_nodes_for_proof(index: i64) -> Vec<i64> {
    let mut indexes = vec![];
    let mut idx = index;
    while idx > 1 {
        if idx % 2 == 0 {
            indexes.push(idx + 1)
        } else {
            indexes.push(idx - 1)
        }
        idx >>= 1
    }
    indexes.push(1);
    indexes
}
