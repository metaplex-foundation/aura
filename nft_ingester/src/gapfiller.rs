use crate::error::IngesterError;
use entities::models::CompleteAssetDetails;
use rocks_db::asset::{AssetCollection, AssetLeaf, Updated};
use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
use serde_json::json;
use std::sync::Arc;

pub fn insert_gap_data(
    rocks_storage: Arc<Storage>,
    data: CompleteAssetDetails,
) -> Result<(), IngesterError> {
    rocks_storage.asset_static_data.merge(
        data.pubkey,
        &AssetStaticDetails {
            pubkey: data.pubkey,
            specification_asset_class: data.specification_asset_class,
            royalty_target_type: data.royalty_target_type,
            created_at: data.created_at,
        },
    )?;

    let chain_data = data.onchain_data.0.map(|d| json!(d).to_string());
    rocks_storage.asset_dynamic_data.merge(
        data.pubkey,
        &AssetDynamicDetails {
            pubkey: data.pubkey,
            is_compressible: Updated::new(data.is_compressible.1, data.is_compressible.0),
            is_compressed: Updated::new(data.is_compressed.1, data.is_compressed.0),
            is_frozen: Updated::new(data.is_frozen.1, data.is_frozen.0),
            supply: Updated::new(data.supply.1, data.supply.0),
            seq: Updated::new(data.seq.1, data.seq.0),
            is_burnt: Updated::new(data.is_burnt.1, data.is_burnt.0),
            was_decompressed: Updated::new(data.was_decompressed.1, data.was_decompressed.0),
            onchain_data: Updated::new(data.onchain_data.1, chain_data),
            creators: Updated::new(data.creators.1, data.creators.0),
            royalty_amount: Updated::new(data.royalty_amount.1, data.royalty_amount.0),
        },
    )?;

    rocks_storage.asset_authority_data.merge(
        data.pubkey,
        &AssetAuthority {
            pubkey: data.pubkey,
            authority: data.authority.0,
            slot_updated: data.authority.1,
        },
    )?;

    rocks_storage.asset_collection_data.merge(
        data.pubkey,
        &AssetCollection {
            pubkey: data.pubkey,
            collection: data.collection.collection,
            is_collection_verified: data.collection.is_collection_verified,
            collection_seq: data.collection.collection_seq,
            slot_updated: data.collection.slot_updated,
        },
    )?;

    data.leaves.iter().try_for_each(|leaf| {
        rocks_storage.asset_leaf_data.merge(
            data.pubkey,
            &AssetLeaf {
                pubkey: data.pubkey,
                tree_id: leaf.tree_id,
                leaf: leaf.leaf.clone(),
                nonce: leaf.nonce,
                data_hash: leaf.data_hash,
                creator_hash: leaf.creator_hash,
                leaf_seq: leaf.leaf_seq,
                slot_updated: leaf.slot_updated,
            },
        )
    })?;

    rocks_storage.asset_owner_data.merge(
        data.pubkey,
        &AssetOwner {
            pubkey: data.pubkey,
            owner: Updated::new(data.owner.1, data.owner.0),
            delegate: Updated::new(data.delegate.1, data.delegate.0),
            owner_type: Updated::new(data.owner_type.1, data.owner_type.0),
            owner_delegate_seq: Updated::new(data.owner_delegate_seq.1, data.owner_delegate_seq.0),
        },
    )?;

    // TODO CLItems

    Ok(())
}
