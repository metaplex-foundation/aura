use std::sync::Arc;

use async_trait::async_trait;
use entities::{
    enums::TokenMetadataEdition,
    models::{AssetCompleteDetailsGrpc, SplMint, Updated},
};
use interface::asset_streaming_and_discovery::{
    AssetDetailsStream, AssetDetailsStreamer, AsyncError,
};
use metrics_utils::red::RequestErrorDurationMetrics;
use rocksdb::DB;
use solana_sdk::pubkey::Pubkey;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    asset::{AssetCompleteDetails, AssetLeaf, SlotAssetIdx, SlotAssetIdxKey},
    cl_items::{ClItemKey, ClLeaf, ClLeafKey},
    column::TypedColumn,
    columns::{
        cl_items::ClItemV2,
        offchain_data::OffChainData,
        parameters::{Parameter, ParameterColumn},
    },
    errors::StorageError,
    generated::asset_generated::asset as fb,
    Storage,
};

const OFFSET_SLOTS: u64 = 300;

#[async_trait]
impl AssetDetailsStreamer for Storage {
    async fn get_asset_details_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, AsyncError> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let backend = self.slot_asset_idx.backend.clone();
        let metrics = self.red_metrics.clone();
        usecase::executor::spawn(async move {
            let _ = process_asset_details_range(
                backend,
                start_slot,
                end_slot,
                metrics.clone(),
                tx.clone(),
            )
            .await;
        });

        Ok(Box::pin(ReceiverStream::new(rx)) as AssetDetailsStream)
    }
}

async fn process_asset_details_range(
    backend: Arc<DB>,
    start_slot: u64,
    end_slot: u64,
    metrics: Arc<RequestErrorDurationMetrics>,
    tx: tokio::sync::mpsc::Sender<Result<AssetCompleteDetailsGrpc, AsyncError>>,
) -> Result<(), AsyncError> {
    let slot_asset_idx = Storage::column::<SlotAssetIdx>(backend.clone(), metrics.clone());
    let iterator = slot_asset_idx
        .iter(SlotAssetIdxKey::new(start_slot, solana_sdk::pubkey::Pubkey::default()));

    for pair in iterator {
        let (idx_key, _) = pair.map_err(|e| Box::new(e) as AsyncError)?;
        let SlotAssetIdxKey { slot, pubkey } =
            SlotAssetIdx::decode_key(idx_key.to_vec()).map_err(|e| Box::new(e) as AsyncError)?;
        if slot > end_slot {
            break;
        }

        let details = get_complete_asset_details(backend.clone(), pubkey, metrics.clone()).await;
        match details {
            Err(e) => {
                if tx.send(Err(Box::new(e) as AsyncError)).await.is_err() {
                    break; // Receiver is dropped
                }
            },
            Ok(details) => {
                if tx.send(Ok(details)).await.is_err() {
                    break; // Receiver is dropped
                }
            },
        }
    }

    Ok(())
}

async fn get_complete_asset_details(
    backend: Arc<DB>,
    pubkey: Pubkey,
    metrics: Arc<RequestErrorDurationMetrics>,
) -> crate::Result<AssetCompleteDetailsGrpc> {
    let data = backend.get_pinned_cf(
        &backend.cf_handle(AssetCompleteDetails::NAME).unwrap(),
        AssetCompleteDetails::encode_key(pubkey),
    )?;
    let data = match data {
        None => {
            return Err(StorageError::Common("Asset data not found".to_string()));
        },
        Some(data) => data,
    };
    let data = fb::root_as_asset_complete_details(&data)
        .map_err(|e| StorageError::Common(e.to_string()))?;
    // TODO: optimization point: this may be optimized by using the flatbuffers directly instead of converting to a struct, skipping for now
    let data = AssetCompleteDetails::from(data);
    let static_data = match data.static_details {
        None => {
            return Err(StorageError::Common("Asset static data not found".to_string()));
        },
        Some(static_data) => static_data,
    };

    let dynamic_data = match data.dynamic_details {
        None => {
            return Err(StorageError::Common("Asset dynamic data not found".to_string()));
        },
        Some(dynamic_data) => dynamic_data,
    };
    let authority = match data.authority {
        None => {
            return Err(StorageError::Common("Asset authority not found".to_string()));
        },
        Some(authority) => authority,
    };
    let owner = match data.owner {
        None => {
            return Err(StorageError::Common("Asset owner not found".to_string()));
        },
        Some(owner) => owner,
    };

    let asset_leaf = Storage::column::<AssetLeaf>(backend.clone(), metrics.clone()).get(pubkey)?;
    let collection = data.collection;

    let onchain_data = match dynamic_data.onchain_data {
        None => None,
        Some(onchain_data) => {
            let v = serde_json::from_str(&onchain_data.value)
                .map_err(|e| StorageError::Common(e.to_string()))?;
            Some(Updated::new(onchain_data.slot_updated, onchain_data.update_version, v))
        },
    };

    let cl_leaf = match asset_leaf {
        None => None,
        Some(ref leaf) => Storage::column::<ClLeaf>(backend.clone(), metrics.clone())
            .get(ClLeafKey::new(leaf.nonce.unwrap_or_default(), leaf.tree_id))?,
    };

    let cl_items = match cl_leaf {
        None => vec![],
        Some(ref leaf) => {
            Storage::column::<ClItemV2>(backend.clone(), metrics.clone())
                .batch_get(
                    get_required_nodes_for_proof(leaf.cli_node_idx as i64)
                        .into_iter()
                        .map(move |node| ClItemKey::new(node as u64, leaf.cli_tree_key))
                        .collect::<Vec<_>>(),
                )
                .await?
        },
    }
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let token_metadata_edition = if let Some(edition_address) = static_data.edition_address {
        Storage::column::<TokenMetadataEdition>(backend.clone(), metrics.clone())
            .get_async(edition_address)
            .await?
    } else {
        None
    };
    let (edition, master_edition) = match token_metadata_edition {
        None => (None, None),
        Some(TokenMetadataEdition::MasterEdition(master_edition)) => (None, Some(master_edition)),
        Some(TokenMetadataEdition::EditionV1(edition)) => {
            let parent = Storage::column::<TokenMetadataEdition>(backend.clone(), metrics.clone())
                .get_async(edition.parent)
                .await?;
            let master_edition =
                if let Some(TokenMetadataEdition::MasterEdition(master_edition)) = parent {
                    Some(master_edition)
                } else {
                    None
                };
            (Some(edition), master_edition)
        },
    };

    let url = dynamic_data.url.clone();
    let spl_mint = Storage::column::<SplMint>(backend.clone(), metrics.clone()).get(pubkey)?;
    let off_chain_data_grpc = Storage::column::<OffChainData>(backend.clone(), metrics.clone())
        .get(url.clone().value)?
        .map(Into::into);
    let cutoff_slot = Storage::column::<ParameterColumn<u64>>(backend.clone(), metrics.clone())
        .get(Parameter::LastBackfilledSlot)?
        .flatten()
        .unwrap_or_default()
        .wrapping_sub(OFFSET_SLOTS);

    Ok(AssetCompleteDetailsGrpc {
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
        url: url.clone(),
        chain_mutability: dynamic_data.chain_mutability,
        lamports: dynamic_data.lamports,
        executable: dynamic_data.executable,
        metadata_owner: dynamic_data.metadata_owner,
        raw_name: dynamic_data.raw_name,
        mpl_core_plugins: dynamic_data.mpl_core_plugins,
        mpl_core_unknown_plugins: dynamic_data.mpl_core_unknown_plugins,
        mpl_core_external_plugins: dynamic_data.mpl_core_external_plugins,
        mpl_core_unknown_external_plugins: dynamic_data.mpl_core_unknown_external_plugins,
        mint_extensions: dynamic_data.mint_extensions,
        rent_epoch: dynamic_data.rent_epoch,
        num_minted: dynamic_data.num_minted,
        current_size: dynamic_data.current_size,
        plugins_json_version: dynamic_data.plugins_json_version,
        authority: Updated::new(authority.slot_updated, None, authority.authority),
        owner_record_pubkey: owner.pubkey,
        owner: owner.owner,
        delegate: owner.delegate,
        owner_type: owner.owner_type,
        owner_delegate_seq: owner.owner_delegate_seq,
        is_current_owner: owner.is_current_owner,
        collection: collection.map(|collection| entities::models::AssetCollection {
            collection: collection.collection,
            is_collection_verified: collection.is_collection_verified,
            authority: collection.authority,
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
                    collection_hash: leaf.collection_hash,
                    asset_data_hash: leaf.asset_data_hash,
                    flags: leaf.flags,
                },
            )
        }),
        cl_items: cl_items
            .into_iter()
            .map(|item| {
                let hash = item.get_updated_hash(cutoff_slot);
                entities::models::ClItem {
                    cli_node_idx: item.node_idx,
                    cli_tree_key: item.tree_key,
                    cli_leaf_idx: item.leaf_idx,
                    cli_seq: hash.get_upd_ver_seq().unwrap_or_default(),
                    cli_level: item.level,
                    cli_hash: hash.value,
                    slot_updated: hash.slot_updated,
                }
            })
            .collect(),
        edition,
        master_edition,
        offchain_data: off_chain_data_grpc,
        spl_mint,
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
