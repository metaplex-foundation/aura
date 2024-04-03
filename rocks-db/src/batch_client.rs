use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use entities::enums::SpecificationVersions;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;

use crate::asset::{AssetCollection, AssetLeaf, AssetsUpdateIdx, SlotAssetIdx};
use crate::cl_items::{ClItem, ClLeaf};
use crate::column::TypedColumn;
use crate::editions::TokenMetadataEdition;
use crate::errors::StorageError;
use crate::key_encoders::{decode_u64x2_pubkey, encode_u64x2_pubkey};
use crate::storage_traits::{
    AssetIndexReader, AssetSlotStorage, AssetUpdateIndexStorage, AssetUpdatedKey,
};
use crate::{
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Result, Storage,
    BATCH_ITERATION_ACTION, ITERATOR_TOP_ACTION, ROCKS_COMPONENT,
};
use entities::models::{AssetIndex, CompleteAssetDetails, UpdateVersion, Updated, UrlWithStatus};

impl AssetUpdateIndexStorage for Storage {
    fn last_known_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>> {
        _ = self.db.try_catch_up_with_primary();
        let start_time = chrono::Utc::now();
        let mut iter = self.assets_update_idx.iter_end();
        if let Some(pair) = iter.next() {
            let (last_key, _) = pair?;
            let key = AssetsUpdateIdx::decode_key(last_key.to_vec())?;
            let decoded_key = decode_u64x2_pubkey(key).unwrap();
            self.red_metrics.observe_request(
                ROCKS_COMPONENT,
                ITERATOR_TOP_ACTION,
                AssetsUpdateIdx::NAME,
                start_time,
            );
            Ok(Some(decoded_key))
        } else {
            Ok(None)
        }
    }

    fn fetch_asset_updated_keys(
        &self,
        from: Option<AssetUpdatedKey>,
        up_to: Option<AssetUpdatedKey>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<AssetUpdatedKey>)> {
        let mut unique_pubkeys = HashSet::new();
        let mut last_key = from;

        if limit == 0 {
            return Ok((unique_pubkeys, last_key));
        }
        let start_time = chrono::Utc::now();

        let iterator = match last_key.clone() {
            Some(key) => {
                let encoded = encode_u64x2_pubkey(key.seq, key.slot, key.pubkey);
                let mut iter = self.assets_update_idx.iter(encoded);
                iter.next(); // Skip the first key, as it is the `from`
                iter
            }
            None => self.assets_update_idx.iter_start(),
        };

        for pair in iterator {
            let (idx_key, _) = pair?;
            let key = AssetsUpdateIdx::decode_key(idx_key.to_vec())?;
            // Stop if the current key is greater than `up_to`
            if let Some(ref up_to_key) = up_to {
                let up_to = encode_u64x2_pubkey(up_to_key.seq, up_to_key.slot, up_to_key.pubkey);
                if key > up_to {
                    break;
                }
            }
            let decoded_key = decode_u64x2_pubkey(key.clone()).unwrap();
            last_key = Some(decoded_key.clone());
            // Skip keys that are in the skip_keys set
            if skip_keys
                .as_ref()
                .map_or(false, |sk| sk.contains(&decoded_key.pubkey))
            {
                continue;
            }

            unique_pubkeys.insert(decoded_key.pubkey);

            if unique_pubkeys.len() >= limit {
                break;
            }
        }
        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            BATCH_ITERATION_ACTION,
            AssetsUpdateIdx::NAME,
            start_time,
        );
        Ok((unique_pubkeys, last_key))
    }
}

#[async_trait]
impl AssetIndexReader for Storage {
    async fn get_asset_indexes(&self, keys: &[Pubkey]) -> Result<HashMap<Pubkey, AssetIndex>> {
        let mut asset_indexes = HashMap::new();
        let start_time = chrono::Utc::now();
        let assets_static_fut = self.asset_static_data.batch_get(keys.to_vec());
        let assets_dynamic_fut = self.asset_dynamic_data.batch_get(keys.to_vec());
        let assets_authority_fut = self.asset_authority_data.batch_get(keys.to_vec());
        let assets_owner_fut = self.asset_owner_data.batch_get(keys.to_vec());
        let assets_collection_fut = self.asset_collection_data.batch_get(keys.to_vec());

        let (
            asset_static_details,
            asset_dynamic_details,
            asset_authority_details,
            asset_owner_details,
            asset_collection_details,
        ) = tokio::join!(
            assets_static_fut,
            assets_dynamic_fut,
            assets_authority_fut,
            assets_owner_fut,
            assets_collection_fut,
        );

        let asset_static_details = asset_static_details?;
        let asset_dynamic_details = asset_dynamic_details?;
        let asset_authority_details = asset_authority_details?;
        let asset_owner_details = asset_owner_details?;
        let asset_collection_details = asset_collection_details?;

        for static_info in asset_static_details.iter().flatten() {
            let asset_index = AssetIndex {
                pubkey: static_info.pubkey,
                specification_version: SpecificationVersions::V1,
                specification_asset_class: static_info.specification_asset_class,
                royalty_target_type: static_info.royalty_target_type,
                slot_created: static_info.created_at,
                ..Default::default()
            };

            asset_indexes.insert(asset_index.pubkey, asset_index);
        }

        for dynamic_info in asset_dynamic_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&dynamic_info.pubkey) {
                existed_index.is_compressible = dynamic_info.is_compressible.value;
                existed_index.is_compressed = dynamic_info.is_compressed.value;
                existed_index.is_frozen = dynamic_info.is_frozen.value;
                existed_index.supply = dynamic_info.supply.clone().map(|s| s.value as i64);
                existed_index.is_burnt = dynamic_info.is_burnt.value;
                existed_index.creators = dynamic_info.creators.clone().value;
                existed_index.royalty_amount = dynamic_info.royalty_amount.value as i64;
                existed_index.slot_updated = dynamic_info.get_slot_updated() as i64;
                existed_index.metadata_url = self.url_with_status_for(dynamic_info);
            } else {
                let asset_index = AssetIndex {
                    pubkey: dynamic_info.pubkey,
                    is_compressible: dynamic_info.is_compressible.value,
                    is_compressed: dynamic_info.is_compressed.value,
                    is_frozen: dynamic_info.is_frozen.value,
                    supply: dynamic_info.supply.clone().map(|s| s.value as i64),
                    is_burnt: dynamic_info.is_burnt.value,
                    creators: dynamic_info.creators.clone().value,
                    royalty_amount: dynamic_info.royalty_amount.value as i64,
                    slot_updated: dynamic_info.get_slot_updated() as i64,
                    metadata_url: self.url_with_status_for(dynamic_info),
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_authority_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.authority = Some(data.authority);
                if data.slot_updated as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.slot_updated as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    authority: Some(data.authority),
                    slot_updated: data.slot_updated as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_owner_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.owner = data.owner.value;
                existed_index.delegate = data.delegate.value;
                existed_index.owner_type = Some(data.owner_type.value);
                if data.get_slot_updated() as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.get_slot_updated() as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    owner: data.owner.value,
                    delegate: data.delegate.value,
                    owner_type: Some(data.owner_type.value),
                    slot_updated: data.get_slot_updated() as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_collection_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.collection = Some(data.collection);
                existed_index.is_collection_verified = Some(data.is_collection_verified);
                if data.slot_updated as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.slot_updated as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    collection: Some(data.collection),
                    is_collection_verified: Some(data.is_collection_verified),
                    slot_updated: data.slot_updated as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }
        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            BATCH_ITERATION_ACTION,
            "collect_asset_indexes",
            start_time,
        );
        Ok(asset_indexes)
    }
}

impl AssetSlotStorage for Storage {
    fn last_saved_slot(&self) -> Result<Option<u64>> {
        let mut iter = self.slot_asset_idx.iter_end();
        if let Some(pair) = iter.next() {
            let (last_key, _) = pair?;
            let (slot, _) = SlotAssetIdx::decode_key(last_key.to_vec())?;
            return Ok(Some(slot));
        }

        Ok(None)
    }
}

impl Storage {
    pub async fn insert_gaped_data(&self, data: CompleteAssetDetails) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        self.asset_static_data.merge_with_batch(
            &mut batch,
            data.pubkey,
            &AssetStaticDetails {
                pubkey: data.pubkey,
                specification_asset_class: data.specification_asset_class,
                royalty_target_type: data.royalty_target_type,
                created_at: data.slot_created as i64,
                edition_address: data.edition_address,
            },
        )?;

        self.asset_dynamic_data.merge_with_batch(
            &mut batch,
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
                        chain_data.update_version,
                        json!(chain_data.value).to_string(),
                    )
                }),
                creators: data.creators,
                royalty_amount: data.royalty_amount,
                url: data.url,
                chain_mutability: data.chain_mutability,
                lamports: data.lamports,
                executable: data.executable,
                metadata_owner: data.metadata_owner,
                raw_name: data.raw_name,
                plugins: data.plugins,
                unknown_plugins: data.unknown_plugins,
                rent_epoch: data.rent_epoch,
                num_minted: data.num_minted,
                current_size: data.current_size,
                plugins_json_version: data.plugins_json_version,
            },
        )?;

        let write_version = if let Some(write_v) = data.authority.update_version {
            match write_v {
                UpdateVersion::WriteVersion(v) => Some(v),
                _ => None,
            }
        } else {
            None
        };

        self.asset_authority_data.merge_with_batch(
            &mut batch,
            data.pubkey,
            &AssetAuthority {
                pubkey: data.pubkey,
                authority: data.authority.value,
                slot_updated: data.authority.slot_updated,
                write_version,
            },
        )?;

        if let Some(collection) = data.collection {
            let write_version = if let Some(write_v) = collection.update_version {
                match write_v {
                    UpdateVersion::WriteVersion(v) => Some(v),
                    _ => None,
                }
            } else {
                None
            };

            self.asset_collection_data.merge_with_batch(
                &mut batch,
                data.pubkey,
                &AssetCollection {
                    pubkey: data.pubkey,
                    collection: collection.value.collection,
                    is_collection_verified: collection.value.is_collection_verified,
                    collection_seq: collection.value.collection_seq,
                    slot_updated: collection.slot_updated,
                    write_version,
                },
            )?;
        }

        if let Some(leaf) = data.asset_leaf {
            self.asset_leaf_data.merge_with_batch(
                &mut batch,
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

        self.asset_owner_data.merge_with_batch(
            &mut batch,
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
            self.cl_leafs.put_with_batch(
                &mut batch,
                (leaf.cli_leaf_idx, leaf.cli_tree_key),
                &ClLeaf {
                    cli_leaf_idx: leaf.cli_leaf_idx,
                    cli_tree_key: leaf.cli_tree_key,
                    cli_node_idx: leaf.cli_node_idx,
                },
            )?
        }
        for item in data.cl_items {
            self.cl_items.merge_with_batch(
                &mut batch,
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
            )?;
        }
        if let Some(edition) = data.edition {
            self.token_metadata_edition_cbor.merge_with_batch_cbor(
                &mut batch,
                edition.key,
                &TokenMetadataEdition::EditionV1(edition),
            )?;
        }
        if let Some(master_edition) = data.master_edition {
            self.token_metadata_edition_cbor.merge_with_batch_cbor(
                &mut batch,
                master_edition.key,
                &TokenMetadataEdition::MasterEdition(master_edition),
            )?;
        }
        self.write_batch(batch).await?;
        Ok(())
    }

    pub(crate) async fn write_batch(&self, batch: rocksdb::WriteBatch) -> Result<()> {
        let backend = self.db.clone();
        tokio::task::spawn_blocking(move || backend.write(batch))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
            .map_err(|e| StorageError::Common(e.to_string()))?;
        Ok(())
    }

    fn url_with_status_for(&self, dynamic_info: &AssetDynamicDetails) -> Option<UrlWithStatus> {
        if dynamic_info.url.value.trim().is_empty() {
            None
        } else {
            Some(UrlWithStatus {
                metadata_url: dynamic_info.url.value.clone(),
                is_downloaded: self
                    .asset_offchain_data
                    .get(dynamic_info.url.value.clone())
                    .ok()
                    .flatten()
                    .is_some(),
            })
        }
    }
}
